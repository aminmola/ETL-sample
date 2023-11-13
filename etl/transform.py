import re
from typing import List
import pandas as pd
import requests
import random


class Transform:
    def __init__(self, ):
        ### Gender Cats
        self.df = pd.read_csv("components/")

    def run(self,):
        out = None

        return out

    def run(self, records: List[dict]):
        for r in records:
            r["Tags"] = self.run(r)
        return records
