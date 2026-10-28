"""
Deterministic RNG hub.
One master seed -> an independent, named stream per component.
Adding a new component never shifts the random numbers of existing ones,
so the same seed always reproduces the same dataset.
"""
import hashlib
import random
import numpy as np


class RngHub:
    def __init__(self, seed: int | None = None):
        self.seed = seed if seed is not None else random.SystemRandom().randrange(2**31)
        self._py: dict[str, random.Random] = {}
        self._np: dict[str, np.random.Generator] = {}

    def _sub_seed(self, name: str) -> int:
        digest = hashlib.sha256(f"{self.seed}:{name}".encode()).digest()
        return int.from_bytes(digest[:8], "big")

    def py(self, name: str) -> random.Random:
        if name not in self._py:
            self._py[name] = random.Random(self._sub_seed(name))
        return self._py[name]

    def np(self, name: str) -> np.random.Generator:
        if name not in self._np:
            self._np[name] = np.random.default_rng(self._sub_seed(name))
        return self._np[name]


_hub: RngHub | None = None


def init(seed: int | None = None) -> RngHub:
    global _hub
    _hub = RngHub(seed)
    return _hub


def hub() -> RngHub:
    return _hub if _hub is not None else init(None)
