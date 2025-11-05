from dpp import V, PREV, ALL, DPP

with DPP(x="4", y="2", z="42") as p:
    p.common(
        ALL >> (lambda param: int(param) + 1) >> ALL
    )
print(p.y)