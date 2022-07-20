class Context:
    def __init__(self,
                 step: str = "",
                 index: int = 0,
                 dataset_uri: str = ""
                 ):
        self.step = step
        self.index = index
        self.dataset_uri = dataset_uri


class PipelineHandler(object):

    def __init__(
            self,
            context: Context,
    ) -> None:
        self.context = context

