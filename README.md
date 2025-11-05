# DPP: Data DPP Package for Python

一个简洁的Python声明式数据处理流水线，专为数据处理、分支合并等复杂场景设计。

## 特性

- 声明式数据流
- 支持元组语法和`>>`调用: 具有伪代码程度的可读性
- 支持多种处理模式：map、all、branch、sequence
- 支持特殊变量的语法糖

## 快速开始

### 安装

- [x] 直接clone到项目目录
- [ ] `pip install .`
- [ ] 发布到 PyPI 后更新安装命令


### 基础使用

```python
from DPP import DPP as P

with P(x = "4", y = "2", z = "42") as p:
    # 使用common统一处理数据，支持lambda表达式和简单函数
    """
    将 x, y, z 转换为整数类型，结果仍存回原变量
    作用等同于：
    x = int(x)
    y = int(y)
    z = int(z)
    """
    p.common(
        (x, y, z), 
        lambda x: int(x), 
        (p.x, p.y, p.z)
    )
    
# with 块外访问结果
print(f"x: {p.x}, y: {p.y}, z: {p.z}")
# x: 4, y: 2, z: 42
```


### 支持`>>`语法

```python
from DPP import V

with P(x = "4", y = "2", z = "42") as p:
    p.common(
        V(x, y, z) >> 
        (lambda x: int(x))  >>
        V(p.x, p.y, p.z)
    )
# 等同于上文基础语法
```


### 处理模式

#### 映射 -- common

将函数逐个应用到输入变量：
```python
_.common((x, y, z), fn, (a, b, c))
_.common(V(x, y, z) >> fn >> V(a, b, c))
# 等价于：a = fn(x), b = fn(y), c = fn(z)
```

#### 聚合 -- all

所有输入一起传给函数：

```python
_.all((x, y, z), fn, (a, b, c))
_.all(V(x, y, z) >> fn >> V(a, b, c))
# 等价于：a, b, c = fn(x, y, z)
```

#### 分支 -- branch

并行执行多个独立分支：

```python
_.branch(
    ((x, y), fn_1, (a, b)),
    (z, fn_2, (c,))
)
_.branch(
    V(x, y) >> fn_1 >> V(a, b),
    z >> fn_2 >> c
)
```

#### 串行 -- sequence

严格按顺序执行：

```python
_.sequence(
    (data, preprocess, (_.cleaned,)),
    (_.cleaned, extract, (_.features,)),
    (_.features, predict, (_.result,))
)

_.sequence(
    data >> preprocess >> _.cleaned,
    _.cleaned >> extract >> _.features,
    _.features >> predict >> _.result
)
```

#### 路由 -- select

根据条件选择分支（仅支持`>>`表达式）：

```python

with P(x = "4", y = "2", z = "42") as p:
    p.select(
        (V(x,y) >> (lambda ctx: int(ctx)+1) >> V(x,y), lambda _: random.random()>0.5),
        (V(z) >> (lambda ctx: int(ctx)+1) >> V(z), lambda _: random.random()<=0.5),
        default=V(x,y) >> (lambda x : int(x)-1) >> V(x,y)
    )
```

### 变量访问

#### 在 with 内

- **输入变量**：直接使用变量名,自动注入
- **输出变量**：通过 `.name` 创建

```python
with DPP(x=10, y=20) as pipe:
    # x, y 可以直接使用（输入变量）
    # pipe.a, pipe.b 创建新的输出变量
    pipe.common((x, y), double, (pipe.a, pipe.b))
```

#### 在 with 块外

通过 DPP 对象直接访问所有变量：

```python
with DPP(x=10, y=20) as pipe:
    pipe.all((x, y), add, (pipe.result,))

# with 块结束后，直接访问
print(pipe.result)  # 30
print(pipe.x)       # 10
print(pipe.y)       # 20
```

#### 保存 DPP 对象

可以返回 DPP 对象供后续使用：

```python
def process_data(data):
    with DPP(input=data) as pipe:
        pipe.common(input, preprocess, (pipe.output,))
    return pipe

# 使用
result = process_data(100)
print(result.output)  # 访问处理结果
```

### 5. 超级符号

#### `PREV` - 上一步的所有出

```python
from DPP import PREV

_.common(V(x, y, z) >> fn1 >> V(a, b, c))
_.common(PREV >> fn2 >> V(d, e, f))  # PREV = [a, b, c]
```

#### `ALL` - 当前上下文的所有变量

```python
from DPP import ALL

_.common(V(ALL) >> normalize >> V(ALL)) 
```

### 链式调用

```python

with P(x = "4", y = "2", z = "42") as p:
    p.common(
        ALL, lambda param: int(param) + 1, ALL
    ).select(
        (V(x,y) >> (lambda ctx: int(ctx)+1) >> V(x,y), lambda _: random.random()>0.5),
        (V(z) >> (lambda ctx: int(ctx)+1) >> V(z), lambda _: random.random()<=0.5),
        default=V(x,y) >> (lambda x : int(x)-1) >> V(x,y)   
    
```

## API Ref

### DPP类

```python
DPP(**initial_data)
```

#### 主要方法

- common
- all
- branch
- sequence
- select
- debug

debug需要在数据流图之前设置

```python
with P(x = "4", y = "2", z = "42") as p:
    p.debug(True)
    p.common(
        ALL, lambda param: int(param) + 1, ALL
    ).select(
        (V(x,y) >> (lambda ctx: int(ctx)+1) >> V(x,y), lambda _: random.random()>0.5),
        (V(z) >> (lambda ctx: int(ctx)+1) >> V(z), lambda _: random.random()<=0.5),
        default=V(x,y) >> (lambda x : int(x)-1) >> V(x,y)   
    )

# 输出调试信息示例
# [MAP] ['x', 'y', 'z'] >> <lambda> >> ['x', 'y', 'z']
# [MAP] ['z'] >> <lambda> >> ['z']
# [SELECT] Executed branch with condition <function <lambda> at 0x1058863a0>
```

#### 单变量简化

```python

# 单变量可以省略包装
x >> fn >> y
# 等价于
V(x) >> fn >> V(y)
```

## 限制

- 占位符注入使用了`sys._getframe`，仅支持CPython
- 新变量需要通过`.name`方式获取
- `select` 条件路由目前仅支持表达式语法

## feature

- [ ] 支持缺省
- [ ] ALL 支持 `>>` 语法
- [ ] 更多超级变量，比如 `EXPECTED`
- [ ] 更多符号变量，比如`...`