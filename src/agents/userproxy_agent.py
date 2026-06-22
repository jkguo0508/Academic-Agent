import asyncio

from autogen_agentchat.agents import UserProxyAgent
from autogen_agentchat.messages import TextMessage
from autogen_core import CancellationToken

from src.infra import settings
from src.infra.event_bus import await_human_input
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)


class WebUserProxyAgent(UserProxyAgent):
    """进程内实现（向后兼容 / 本地调试）。

    依赖 asyncio.Future，只能在同一事件循环/进程内工作。
    原版问题：on_messages 的 await 无超时，人不输入会永久阻塞。这里加上超时。
    """

    def __init__(self, name):
        super().__init__(name)
        self.waiting_future = None

    async def on_messages(self, messages, cancellation_token: CancellationToken):
        self.waiting_future = asyncio.get_event_loop().create_future()
        try:
            user_input = await asyncio.wait_for(
                self.waiting_future, timeout=settings.HUMAN_INPUT_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.warning("等待人工输入超时，使用空输入继续")
            user_input = ""
        return TextMessage(content=user_input, source="human")

    def set_user_input(self, user_input: str):
        if self.waiting_future and not self.waiting_future.done():
            self.waiting_future.set_result(user_input)


class RedisUserProxyAgent(UserProxyAgent):
    """分布式实现：跨进程人工审核。

    Worker 侧使用。on_messages 通过 Redis BLPOP 阻塞等待前端输入，
    网关的 /send_input 接口通过 RPUSH 唤醒。带超时，避免任务/上下文泄漏。
    """

    def __init__(self, name, task_id: str):
        super().__init__(name)
        self.task_id = task_id

    async def on_messages(self, messages, cancellation_token: CancellationToken):
        user_input = await await_human_input(self.task_id)
        if user_input is None:
            # 超时后以空输入继续，保证流水线不会死锁
            logger.warning("task_id=%s 等待人工输入超时，使用空输入继续", self.task_id)
            user_input = ""
        return TextMessage(content=user_input, source="human")

    def set_user_input(self, user_input: str):  # 兼容接口（实际走 Redis）
        raise NotImplementedError("RedisUserProxyAgent 通过 Redis 接收输入")


# 向后兼容：保留全局实例（仅用于本地回退，生产路径不使用）
userProxyAgent = WebUserProxyAgent("user_proxy")
