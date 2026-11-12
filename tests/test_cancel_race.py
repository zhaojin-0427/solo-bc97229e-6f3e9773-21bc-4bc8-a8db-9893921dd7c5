"""取消与任务结束竞态的回归测试（不依赖真实浏览器，可重复执行）。

用可控 FakeBot 替换 PortalBot，再以事件门闩 + 条件迁移包装精确制造
「取消 vs 任务结束」的交错时序：
- 文件已通过最后检查、发布状态尚未写入时取消到达（取消必须生效）；
- 取消受理后浏览器异常才返回（不得改成失败、不得留下工作文件）；
- 复制文件期间状态查询与取消请求仍可响应；
- 发布先生效则取消被拒绝、文件保留。
"""
from __future__ import annotations

import random
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "collector"))

import app.worker as worker_module  # noqa: E402
from app import config  # noqa: E402
from app.models import FINAL_STATES, TaskStore  # noqa: E402
from app.web import create_app  # noqa: E402
from app.worker import Worker  # noqa: E402


# ---------------------------------------------------------------------------
# 假浏览器与行为脚本
# ---------------------------------------------------------------------------
class _FakeBot:
    """假浏览器：run() 行为由测试注入，签名与 PortalBot 一致。"""

    def __init__(self, behavior, task, work_dir, cancel_event):
        self._behavior = behavior
        self.task = task
        self.work_dir = Path(work_dir)
        self.cancel_event = cancel_event

    def run(self):
        return self._behavior(self.task, self.work_dir, self.cancel_event)


def bot_factory(behavior):
    def factory(task, work_dir, cancel_event):
        return _FakeBot(behavior, task, work_dir, cancel_event)

    return factory


def quick_success(task, work_dir, cancel_event):
    """立即产出一份与任务匹配的合法 CSV。"""
    path = work_dir / "export.csv"
    content = (
        "产线,日期,样品号,测量值\n"
        f"{task['line']},{task['date']},S-001,12.34\n"
        f"{task['line']},{task['date']},S-002,56.78\n"
    )
    path.write_text(content, encoding="utf-8")
    return path


def raise_after_cancel(task, work_dir, cancel_event):
    """等取消信号到达后才抛出浏览器异常（模拟迟到的异常返回）。"""
    if not cancel_event.wait(10):
        raise AssertionError("取消信号未在 10 秒内到达")
    raise RuntimeError("浏览器崩溃：CDP 连接断开")


def gated_success(entered: threading.Event, release: threading.Event):
    """进入后挂起，直到测试放行；用于把任务稳定在 running 态。"""

    def behavior(task, work_dir, cancel_event):
        entered.set()
        if not release.wait(10):
            raise AssertionError("测试未在 10 秒内放行")
        return quick_success(task, work_dir, cancel_event)

    return behavior


# ---------------------------------------------------------------------------
class CancelRaceTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="cancel-race-"))
        self._saved_config = {
            key: getattr(config, key)
            for key in ("DATA_DIR", "DB_PATH", "PUBLISHED_DIR", "WORK_DIR")
        }
        config.DATA_DIR = self.tmpdir / "data"
        config.DB_PATH = config.DATA_DIR / "collector.db"
        config.PUBLISHED_DIR = config.DATA_DIR / "published"
        config.WORK_DIR = config.DATA_DIR / "work"
        self.store = TaskStore(str(config.DB_PATH))
        self._workers: list[Worker] = []

    def tearDown(self):
        for worker in self._workers:
            worker.shutdown()
        for worker in self._workers:
            worker.join(timeout=10)
        self.store.close()
        for key, value in self._saved_config.items():
            setattr(config, key, value)
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # ---- 辅助 -------------------------------------------------------------
    def make_worker(self, behavior, start=True) -> Worker:
        worker = Worker(self.store, bot_factory=bot_factory(behavior))
        self._workers.append(worker)
        if start:
            worker.start()
        return worker

    def wait_status(self, task_id, targets, timeout=10):
        deadline = time.monotonic() + timeout
        task = None
        while time.monotonic() < deadline:
            task = self.store.get_task(task_id)
            if task["status"] in targets:
                return task
            time.sleep(0.02)
        self.fail(f"等待状态 {targets} 超时，当前 {task and task['status']}")

    def wait_final(self, task_id, timeout=10):
        return self.wait_status(task_id, FINAL_STATES, timeout)

    def published_files(self, task_id):
        return list(config.PUBLISHED_DIR.glob(f"*{task_id}*"))

    def wait_work_dir_cleaned(self, task_id, timeout=5):
        # 发布成功时工作目录的清理紧随状态写库，给它一点收敛时间。
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not (config.WORK_DIR / task_id).exists():
                return
            time.sleep(0.02)
        self.fail(f"任务 {task_id} 的工作目录未清理")

    def assert_task_files_consistent(self, task):
        """终态与文件系统必须一致：published ⇔ 质量库有文件；cancelled ⇔ 无残留。"""
        task_id = task["id"]
        files = self.published_files(task_id)
        if task["status"] == "published":
            self.wait_work_dir_cleaned(task_id)
            self.assertEqual(len(files), 1, "已发布任务必须保留质量库文件")
        elif task["status"] == "cancelled":
            self.assertEqual(files, [], "已取消任务不得留下质量库文件")
            self.assertFalse(
                (config.WORK_DIR / task_id).exists(),
                "已取消任务不得留下工作文件",
            )
        else:
            self.fail(f"本用例不应出现终态 {task['status']}")

    # ---- 竞态回归 ---------------------------------------------------------
    def test_cancel_inside_publish_window_ends_cancelled(self):
        """文件已通过最后检查、发布状态尚未写入时取消到达：
        取消受理后，发布不得再生效，任务最终为已取消。"""
        self.make_worker(quick_success)
        original = self.store.try_mark_published
        cancel_outcome = {}

        def racing_publish(task_id, file_name):
            # 取消恰好落在「最后检查通过」与「写入发布状态」之间
            cancel_outcome["ok"], cancel_outcome["message"] = (
                self.store.request_cancel(task_id)
            )
            return original(task_id, file_name)

        with mock.patch.object(self.store, "try_mark_published", racing_publish):
            task = self.store.create_task("A线", "2026-11-10")
            self._workers[-1].notify()
            final = self.wait_final(task["id"])

        self.assertTrue(cancel_outcome["ok"], cancel_outcome)  # 取消已受理
        self.assertEqual(final["status"], "cancelled")  # 最终必须已取消
        self.assertIsNone(final["published_file"])
        self.assert_task_files_consistent(final)

    def test_api_cancel_during_publish_write_then_download_refused(self):
        """HTTP 层复现：取消接口在发布窗口内返回成功，
        任务随后不得变为已发布，下载必须被拒绝。"""
        worker = self.make_worker(quick_success, start=False)
        app = create_app(store=self.store, worker=worker)
        client = app.test_client()

        publish_entered = threading.Event()
        release_publish = threading.Event()
        original = self.store.try_mark_published

        def gated_publish(task_id, file_name):
            publish_entered.set()
            if not release_publish.wait(10):
                raise AssertionError("测试未放行发布")
            return original(task_id, file_name)

        with mock.patch.object(self.store, "try_mark_published", gated_publish):
            resp = client.post(
                "/api/tasks",
                data={"line": "A线", "date": "2026-11-10"},
                headers={"Accept": "application/json"},
            )
            self.assertEqual(resp.status_code, 201)
            task_id = resp.get_json()["id"]

            # 等 worker 进入「文件已复制、发布状态未写入」的窗口
            self.assertTrue(publish_entered.wait(10))
            # 窗口内状态查询必须可响应
            status_resp = client.get(f"/api/tasks/{task_id}")
            self.assertEqual(status_resp.status_code, 200)
            # 取消接口返回成功
            cancel_resp = client.post(f"/api/tasks/{task_id}/cancel")
            self.assertEqual(cancel_resp.status_code, 200)
            self.assertTrue(cancel_resp.get_json()["ok"])
            release_publish.set()
            final = self.wait_final(task_id)

        self.assertEqual(final["status"], "cancelled")
        self.assert_task_files_consistent(final)
        # 下载接口必须拒绝
        self.assertEqual(client.get(f"/tasks/{task_id}/download").status_code, 409)

    def test_late_browser_exception_after_cancel_stays_cancelled(self):
        """取消受理后浏览器异常才返回：不得改成失败、不得留下工作文件；
        清理完成后队列交接门户会话，后续任务正常采集。"""
        calls = []

        def first_raises_then_success(task, work_dir, cancel_event):
            calls.append(task["id"])
            if len(calls) == 1:
                return raise_after_cancel(task, work_dir, cancel_event)
            return quick_success(task, work_dir, cancel_event)

        worker = self.make_worker(first_raises_then_success)
        task = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        self.wait_status(task["id"], ("running",))

        ok, _ = self.store.request_cancel(task["id"])
        self.assertTrue(ok)
        final = self.wait_final(task["id"])
        self.assertEqual(final["status"], "cancelled")  # 异常不得改写为 failed
        self.assertIn("删除未发布文件", final["message"])
        self.assert_task_files_consistent(final)

        # 取消清理完成后，后续任务仍可正常采集（会话交接正常）
        nxt = self.store.create_task("B线", "2026-11-11")
        worker.notify()
        followup = self.wait_final(nxt["id"])
        self.assertEqual(followup["status"], "published")
        self.assert_task_files_consistent(followup)

    def test_publish_first_then_cancel_refused_and_file_kept(self):
        """发布先生效：取消被拒绝，质量库文件保留、可下载。"""
        worker = self.make_worker(quick_success, start=False)
        app = create_app(store=self.store, worker=worker)
        client = app.test_client()

        resp = client.post(
            "/api/tasks",
            data={"line": "A线", "date": "2026-11-10"},
            headers={"Accept": "application/json"},
        )
        task_id = resp.get_json()["id"]
        final = self.wait_final(task_id)
        self.assertEqual(final["status"], "published")

        # 存储层与 API 层都必须拒绝取消
        ok, message = self.store.request_cancel(task_id)
        self.assertFalse(ok)
        self.assertIn("已发布", message)
        resp = client.post(f"/api/tasks/{task_id}/cancel")
        self.assertEqual(resp.status_code, 400)

        # 文件保留、状态不被改写、下载正常
        self.assertEqual(self.store.get_task(task_id)["status"], "published")
        self.assert_task_files_consistent(final)
        download = client.get(f"/tasks/{task_id}/download")
        self.assertEqual(download.status_code, 200)
        self.assertIn("产线,日期,样品号,测量值", download.get_data(as_text=True))
        download.close()

    def test_status_and_cancel_responsive_during_file_copy(self):
        """复制文件期间：状态查询与取消请求不得被阻塞，取消仍然生效。"""
        worker = self.make_worker(quick_success, start=False)
        app = create_app(store=self.store, worker=worker)
        client = app.test_client()

        copy_entered = threading.Event()
        release_copy = threading.Event()
        real_copy2 = shutil.copy2

        def slow_copy2(src, dst):
            copy_entered.set()
            if not release_copy.wait(10):
                raise AssertionError("测试未放行复制")
            return real_copy2(src, dst)

        with mock.patch.object(worker_module.shutil, "copy2") as mock_copy:
            mock_copy.side_effect = slow_copy2
            resp = client.post(
                "/api/tasks",
                data={"line": "A线", "date": "2026-11-10"},
                headers={"Accept": "application/json"},
            )
            task_id = resp.get_json()["id"]
            # worker 正卡在复制质量库文件
            self.assertTrue(copy_entered.wait(10))

            start = time.monotonic()
            status_resp = client.get(f"/api/tasks/{task_id}")
            cancel_resp = client.post(f"/api/tasks/{task_id}/cancel")
            elapsed = time.monotonic() - start

            self.assertEqual(status_resp.status_code, 200)
            self.assertEqual(status_resp.get_json()["status"], "running")
            self.assertEqual(cancel_resp.status_code, 200)
            self.assertTrue(cancel_resp.get_json()["ok"])
            # 若复制期间持锁阻塞，两个请求会被卡住直到门闩超时（10 秒）
            self.assertLess(elapsed, 5, "复制文件期间状态查询/取消请求被阻塞")

            release_copy.set()
            final = self.wait_final(task_id)

        self.assertEqual(final["status"], "cancelled")
        self.assert_task_files_consistent(final)
        self.assertEqual(client.get(f"/tasks/{task_id}/download").status_code, 409)

    def test_unexpected_copy_error_with_cancel_ends_cancelled(self):
        """复制文件时发生未捕获异常（如磁盘错误）且取消已受理：
        最终必须为已取消、文件全部清除；worker 存活，后续任务正常。"""
        worker = self.make_worker(quick_success, start=False)
        task = self.store.create_task("A线", "2026-11-10")
        cancel_outcome = {}

        def exploding_copy(src, dst):
            # 磁盘错误发生的同一刻，取消恰好被受理
            cancel_outcome["ok"], _ = self.store.request_cancel(task["id"])
            raise OSError("磁盘只读")

        with mock.patch.object(worker_module.shutil, "copy2") as mock_copy:
            mock_copy.side_effect = exploding_copy
            worker.start()
            worker.notify()
            final = self.wait_final(task["id"])

        self.assertTrue(cancel_outcome["ok"], cancel_outcome)
        self.assertEqual(final["status"], "cancelled")  # 异常不得改写取消
        self.assert_task_files_consistent(final)

        # worker 未被拖垮：后续任务正常采集
        nxt = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        done = self.wait_final(nxt["id"])
        self.assertEqual(done["status"], "published")
        self.assert_task_files_consistent(done)

    def test_unexpected_copy_error_marks_failed_and_worker_continues(self):
        """复制文件时发生未捕获异常（无取消）：任务标记失败可重试，
        质量库目录不得残留该任务文件，后续任务正常。"""
        worker = self.make_worker(quick_success, start=False)
        with mock.patch.object(
            worker_module.shutil, "copy2", side_effect=OSError("磁盘只读")
        ):
            worker.start()
            task = self.store.create_task("A线", "2026-11-10")
            worker.notify()
            final = self.wait_final(task["id"])

        self.assertEqual(final["status"], "failed")
        self.assertIn("内部错误", final["message"])
        self.assertEqual(self.published_files(task["id"]), [])

        nxt = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        done = self.wait_final(nxt["id"])
        self.assertEqual(done["status"], "published")
        self.assert_task_files_consistent(done)

    def test_worker_survives_bot_crash_and_queue_continues(self):
        """浏览器操作抛异常（如启动失败）后：任务标记失败可重试，
        worker 不被拖垮，后续任务仍可正常采集。"""
        calls = []

        def first_crashes_then_success(task, work_dir, cancel_event):
            calls.append(task["id"])
            if len(calls) == 1:
                raise RuntimeError("浏览器启动失败：CDP 不可达")
            return quick_success(task, work_dir, cancel_event)

        worker = self.make_worker(first_crashes_then_success)
        bad = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        failed = self.wait_final(bad["id"])
        self.assertEqual(failed["status"], "failed")
        self.assertIn("CDP", failed["message"])

        followup = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        done = self.wait_final(followup["id"])
        self.assertEqual(done["status"], "published")
        self.assert_task_files_consistent(done)

    def test_cancel_queued_task_then_running_task_publishes(self):
        """排队任务取消立即生效；执行中的任务不受影响、正常发布。"""
        entered = threading.Event()
        release = threading.Event()
        worker = self.make_worker(gated_success(entered, release))

        running = self.store.create_task("A线", "2026-11-10")
        worker.notify()
        self.assertTrue(entered.wait(10))  # 第一个任务稳定在执行中

        queued = self.store.create_task("B线", "2026-11-10")
        ok, _ = self.store.request_cancel(queued["id"])
        self.assertTrue(ok)
        final = self.wait_final(queued["id"])
        self.assertEqual(final["status"], "cancelled")
        self.assertIn("排队中取消", final["message"])

        release.set()
        done = self.wait_final(running["id"])
        self.assertEqual(done["status"], "published")
        self.assert_task_files_consistent(done)

    def test_cancel_publish_interleaving_sweep(self):
        """取消在发布窗口前后的不同时刻到达：无论谁先生效，
        「取消受理 ⇔ 最终已取消」「发布生效 ⇔ 拒绝取消」必须成立。"""
        window = 0.08  # 发布写库前的人为窗口（秒）
        worker = self.make_worker(quick_success)  # 单 worker，保证串行
        # 确定性扫掠：窗口内（取消必胜）与窗口外（发布必胜）都覆盖
        for delay in (0.0, 0.01, 0.03, 0.15, 0.30):
            with self.subTest(delay=delay):
                self._run_interleaving_round(worker, delay, window)
        # 固定种子的随机交错：可重复地反复触发竞态
        rng = random.Random(20261112)
        for _ in range(10):
            self._run_interleaving_round(worker, rng.uniform(0, 0.2), window)

    # ------------------------------------------------------------------
    def _run_interleaving_round(self, worker, delay: float, window: float) -> None:
        entered = threading.Event()
        original = self.store.try_mark_published

        def gated_publish(task_id, file_name):
            entered.set()
            time.sleep(window)  # 人为拉大「检查通过 → 写发布状态」窗口
            return original(task_id, file_name)

        with mock.patch.object(self.store, "try_mark_published", gated_publish):
            task = self.store.create_task("A线", "2026-11-10")
            worker.notify()
            self.assertTrue(entered.wait(10))
            time.sleep(delay)
            cancel_ok, _ = self.store.request_cancel(task["id"])
            final = self.wait_final(task["id"])

        if cancel_ok:
            # 取消先受理 ⇒ 最终必须已取消，成功不得改写，文件全部清除
            self.assertEqual(
                final["status"], "cancelled",
                f"delay={delay}: 取消已受理但任务终态为 {final['status']}",
            )
        else:
            # 发布先生效 ⇒ 拒绝取消，文件保留
            self.assertEqual(
                final["status"], "published",
                f"delay={delay}: 取消被拒绝但任务终态为 {final['status']}",
            )
        self.assert_task_files_consistent(final)


if __name__ == "__main__":
    unittest.main(verbosity=2)
