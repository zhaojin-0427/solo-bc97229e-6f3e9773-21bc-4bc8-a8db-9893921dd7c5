"""单线程 worker：串行执行任务，保证门户单会话不被交叉占用。

取消与发布的竞态约定（与 models.TaskStore 的条件迁移配合）：
- 状态列是唯一裁决点：try_mark_published / try_mark_failed 仅当任务仍为
  running 才生效；取消先受理（cancelling/cancelled）时它们返回 False，
  worker 必须撤回已复制文件并按取消收尾，最终状态只能是 cancelled。
- 取消后才返回的浏览器异常不得把任务改成 failed，也不得留下工作文件。
- 清理（删除未发布文件）完成后 worker 才取下一个任务，门户会话不交叉。
"""
from __future__ import annotations

import logging
import shutil
import threading
from pathlib import Path

from . import config
from .models import TaskStore
from .portal_bot import PortalBot, TaskCancelled
from .validation import validate_csv

log = logging.getLogger("collector.worker")


class Worker(threading.Thread):
    def __init__(self, store: TaskStore, bot_factory=None):
        super().__init__(name="collector-worker", daemon=True)
        self.store = store
        # 浏览器操作构造器，测试可注入假 bot 以精确复现竞态时序。
        self._bot_factory = bot_factory or PortalBot
        self._wake = threading.Event()
        # 注意不能叫 _stop：会遮蔽 threading.Thread._stop() 使 join() 崩溃。
        self._stop_event = threading.Event()

    def notify(self) -> None:
        """有新任务或状态变化时唤醒 worker。"""
        self._wake.set()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._wake.set()

    def run(self) -> None:
        log.info("采集 worker 已启动，串行调度门户任务")
        while not self._stop_event.is_set():
            self._wake.wait(config.POLL_INTERVAL)
            self._wake.clear()
            try:
                task = self.store.next_queued()
            except Exception:
                log.exception("读取待执行任务失败")
                continue
            if task is None:
                continue
            try:
                self._run_task(task)
            except Exception:
                # 未捕获异常（如磁盘错误）不得拖垮 worker、也不得覆盖已受理的取消。
                log.exception("任务 %s 执行出现未捕获异常", task["id"])
                try:
                    if not self.store.try_mark_failed(
                        task["id"], "采集服务内部错误，任务中断，可重试"
                    ):
                        self.store.finalize_cancelled(task["id"])
                    self._cleanup_unpublished(task["id"])
                except Exception:
                    log.exception("任务 %s 异常收尾失败", task["id"])

    # ------------------------------------------------------------------
    def _run_task(self, task: dict) -> None:
        task_id = task["id"]
        cancel_event = self.store._event(task_id)  # 复用 models 中的事件

        # 原子抢占 running：排队期间被取消的任务在此出局。
        if not self.store.try_mark_running(task_id):
            log.info("任务 %s 在排队期间已被取消，跳过执行", task_id)
            return
        fresh = self.store.get_task(task_id)
        log.info("任务 %s 开始执行：line=%s date=%s", task_id, fresh["line"], fresh["date"])

        work_dir = config.WORK_DIR / task_id
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        work_dir.mkdir(parents=True, exist_ok=True)

        bot = self._bot_factory(fresh, work_dir, cancel_event)
        try:
            downloaded = bot.run()
        except TaskCancelled:
            self._handle_cancel(task_id, work_dir)
            return
        except Exception as exc:  # 浏览器/门户侧任何异常
            if self._cancel_requested(task_id, cancel_event):
                # 取消后才返回的异常：不得改成 failed，按取消清理。
                log.warning(
                    "任务 %s 取消受理后浏览器异常返回（%s: %s），按取消完成清理",
                    task_id, exc.__class__.__name__, exc,
                )
                self._handle_cancel(task_id, work_dir)
            else:
                log.exception("任务 %s 浏览器操作失败", task_id)
                self._handle_failure(
                    task_id, work_dir,
                    f"门户操作失败：{exc.__class__.__name__}: {exc}",
                )
            return

        # 下载完成后核对：取消信号可能在落盘瞬间到达，
        # 这类「迟到下载」一律不得发布。
        if self._cancel_requested(task_id, cancel_event):
            log.warning("任务 %s 下载已完成但任务已被取消，丢弃迟到下载", task_id)
            self._handle_cancel(task_id, work_dir)
            return

        result = validate_csv(downloaded, fresh["line"], fresh["date"])
        if not result.ok:
            log.warning("任务 %s 校验未通过：%s", task_id, result.message)
            self._handle_failure(task_id, work_dir, f"数据校验失败：{result.message}")
            return

        # 校验期间也可能收到取消：复制前再核对，避免无谓写入质量库目录。
        if self._cancel_requested(task_id, cancel_event):
            log.warning("任务 %s 校验通过但已被取消，丢弃文件", task_id)
            self._handle_cancel(task_id, work_dir)
            return

        published_name = f"{fresh['line']}_{fresh['date']}_{task_id}.csv"
        published_path = config.PUBLISHED_DIR / published_name
        shutil.copy2(downloaded, published_path)

        # 发布是唯一裁决点：仅当状态仍为 running 时条件更新才生效。
        # 取消先受理则此处失败——已复制进质量库目录的文件必须撤回，
        # 任务最终为 cancelled，且不得留下任何未发布文件。
        if not self.store.try_mark_published(task_id, published_name):
            published_path.unlink(missing_ok=True)
            log.warning("任务 %s 发布前被取消，已撤回质量库文件", task_id)
            self._handle_cancel(task_id, work_dir)
            return

        shutil.rmtree(work_dir, ignore_errors=True)
        log.info("任务 %s 已发布：%s（%s）", task_id, published_name, result.message)

    # ------------------------------------------------------------------
    def _cancel_requested(self, task_id: str, cancel_event: threading.Event) -> bool:
        """取消是否已受理（事件或状态任一即可，状态以 DB 为准）。"""
        if cancel_event.is_set():
            return True
        task = self.store.get_task(task_id)
        return task is None or task["status"] != "running"

    def _handle_cancel(self, task_id: str, work_dir: Path) -> None:
        # 先清掉未发布的工作文件，再让下一个任务使用门户会话。
        shutil.rmtree(work_dir, ignore_errors=True)
        self.store.finalize_cancelled(
            task_id, "已停止浏览器操作并删除未发布文件，门户会话已释放"
        )
        log.info("任务 %s 取消完成，允许下一个任务接管会话", task_id)

    def _handle_failure(self, task_id: str, work_dir: Path, message: str) -> None:
        # 取消先受理时失败不得覆盖取消：改为完成取消清理、删除工作文件。
        if self.store.try_mark_failed(task_id, message):
            # 工作目录保留以便排查，但不会对外提供下载。
            log.info("任务 %s 失败（可重试）：%s", task_id, message)
        else:
            log.warning("任务 %s 失败到达前取消已受理，按取消完成清理", task_id)
            self._handle_cancel(task_id, work_dir)

    def _cleanup_unpublished(self, task_id: str) -> None:
        """异常收尾兜底：任务未发布时，清除其在质量库目录的残留文件；
        已取消任务连同工作目录一并清除（失败任务的工作目录保留以便排查）。"""
        task = self.store.get_task(task_id)
        if task is None or task["status"] == "published":
            return
        for stray in config.PUBLISHED_DIR.glob(f"*{task_id}*"):
            stray.unlink(missing_ok=True)
        if task["status"] == "cancelled":
            shutil.rmtree(config.WORK_DIR / task_id, ignore_errors=True)
