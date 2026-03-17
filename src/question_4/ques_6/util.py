
import re

def to_snake_case(df):
    def convert(col):
        return re.sub(r'([a-z])([A-Z])', r'\1_\2', col).lower()

    return df.toDF(*[convert(c) for c in df.columns])