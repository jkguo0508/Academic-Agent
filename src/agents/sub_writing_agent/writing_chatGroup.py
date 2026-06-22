from src.core.model_client import create_default_client
from autogen_agentchat.teams import SelectorGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from src.agents.sub_writing_agent.writing_agent import create_writing_agent
from src.agents.sub_writing_agent.retrieval_agent import create_retrieval_agent
from src.agents.sub_writing_agent.review_agent import create_review_agent
from src.core.prompts import selector_prompt
from src.core.config import config

def create_writing_group():
    model_client = create_default_client()

    text_termination = TextMentionTermination("APPROVE")
    # 为写作组设置最大轮数上限，防止 APPROVE 始终未出现时陷入无限循环
    max_turns = config.get_int("writing.max_turns", 12)
    
    writing_agent = create_writing_agent()
    review_agent = create_review_agent()
    retrieval_agent = create_retrieval_agent()

    task_group = SelectorGroupChat(
        [writing_agent,retrieval_agent,review_agent],
        model_client=model_client,
        termination_condition=text_termination,
        selector_prompt=selector_prompt,
        allow_repeated_speaker=False,  # Allow an agent to speak multiple turns in a row.
        max_turns=max_turns,  # 兜底终止条件，保证鲁棒性
    )
    return task_group
