from metaflow.cards import MetaflowCard


class TestMockCard(MetaflowCard):
    type = "card_init"

    def __init__(self, options={"key": "task"}, **kwargs):
        self._key = options["task"]

    def render(self, task):
        task_data = task[self._key].data
        return "%s" % task_data


CARDS = [TestMockCard]
