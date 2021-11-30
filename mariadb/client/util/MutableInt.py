class MutableInt:
    __slots__ = ('value')
    def __init__(self):
        self.value = -1

    def increment_and_get(self) -> int:
        self.value += 1
        return self.value
