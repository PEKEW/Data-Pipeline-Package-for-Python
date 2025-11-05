"""
Author: peike wang
Date: 2025-11-05
Data DPP Package for Python (DPP)
"""

import sys
from typing import Any, Callable, List, Optional, Union, Dict


# 表达式类
class PartialExpression:
    """
    部分表达式：[inputs] >> fn
    已经指定了输入和处理函数，但还没有指定输出的中间状态。
    通过 >> 操作符继续构建完整表达式。
    Attributes:
        inputs: 输入占位符列表
        fn: 处理函数
    """

    def __init__(self, inputs: List['Placeholder'], fn: Callable):
        """
        初始化部分表达式
        Args:
            inputs: 输入占位符列表
            fn: 处理函数
        """
        self.inputs = inputs if isinstance(inputs, list) else [inputs]
        self.fn = fn

    def __rshift__(self, outputs: Union['Placeholder', List['Placeholder']]) -> 'CompleteExpression':
        """
        >> 添加输出，构建完整表达式

        Args:
            outputs: 输出占位符或占位符列表

        Returns:
            CompleteExpression: 完整的表达式对象
        """
        if not isinstance(outputs, list):
            outputs = [outputs]
        return CompleteExpression(self.inputs, self.fn, outputs)

    def __repr__(self):
        return f"PartialExpression({self.inputs} >> {self.fn.__name__})"


class CompleteExpression:
    """
    [inputs] >> fn >> [outputs]

    表示一个完整的数据处理单元，包含输入、处理函数和输出。

    Attributes:
        inputs: 输入占位符列表
        fn: 处理函数
        outputs: 输出占位符列表
    """

    def __init__(self, inputs: List['Placeholder'], fn: Callable, outputs: List['Placeholder']):
        """
        初始化完整表达式

        Args:
            inputs: 输入占位符列表
            fn: 处理函数
            outputs: 输出占位符列表
        """
        self.inputs = inputs
        self.fn = fn
        self.outputs = outputs

    def __repr__(self):
        return f"CompleteExpression({self.inputs} >> {self.fn.__name__} >> {self.outputs})"


# 占位符类

class Placeholder:
    """
    占位符对象
    Attributes:
        name: 变量名
        DPP: 所属的DPP对象
    """

    def __init__(self, name: str, DPP: Optional['DPP'] = None):
        """
        初始化占位符

        Args:
            name: 变量名
            DPP: 所属的DPP对象（可选）
        """
        self.name = name
        self.DPP = DPP

    def __rshift__(self, other: Callable) -> Union[PartialExpression, CompleteExpression]:
        """
        Args:
            other: 
                函数用来构建PE
        Returns:
            PartialExpression 或 CompleteExpression
        """
        if callable(other):
            # x >> fn -> PartialExpression
            return PartialExpression([self], other)
        else:
            raise TypeError(f"Unsupported operation: Placeholder >> {type(other)}")

    def __repr__(self):
        return f"Placeholder({self.name})"


class PlaceholderList(list):
    """
    占位符列表
    支持V(x, y, z) >> fn 语法构建部分表达式
    """

    def __rshift__(self, fn: Callable) -> PartialExpression:
        """
        支持 >> 操作符，构建部分表达式

        Args:
            fn: 处理函数

        Returns:
            PartialExpression: 部分表达式对象
        """
        return PartialExpression(self, fn)

    def __repr__(self):
        return f"PlaceholderList([{', '.join(p.name for p in self)}])"


# 超级变量

class AllPlaceholder:
    """
    所有当前变量
    """
    def __rshift__(self, fn: Callable) -> PartialExpression:
        return PartialExpression([self], fn)
    def __repr__(self):
        return "*ALL*"


class PrevPlaceholder:
    """
    上一步的输出
    """

    def __rshift__(self, fn: Callable) -> PartialExpression:
        return PartialExpression([self], fn)

    def __repr__(self):
        return "PREV"


ALL = AllPlaceholder()
PREV = PrevPlaceholder()


# DPP 主类

class DPP:
    def __init__(self, **initial_data):
        """
        初始化DPP
        Args:
            **initial_data: 变量名和初始数据的映射
        """
        self.var_names = list(initial_data.keys())
        self.context = initial_data.copy()
        self.placeholders: Dict[str, Placeholder] = {}
        self.last_outputs: List[Placeholder] = []

        self._debug = False
        self._in_context = False

    def __enter__(self) -> 'DPP':
        """
        进入with块时自动创建占位符并注入到调用者命名空间
        """
        self._in_context = True

        # 创建占位符
        for name in self.var_names:
            placeholder = Placeholder(name, self)
            self.placeholders[name] = placeholder

        # 注入到调用者的局部命名空间
        # 使用exec在调用者的命名空间中创建变量
        frame = sys._getframe(1)
        for name, placeholder in self.placeholders.items():
            # 直接在调用者的globals和locals中创建变量
            frame.f_globals[name] = placeholder
            if frame.f_locals is not frame.f_globals:
                frame.f_locals[name] = placeholder

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_context = False
        return False

    def _convert_to_placeholder_list(self, obj: Union[Placeholder, tuple, list]) -> List[Placeholder]:
        """
        将各种输入格式转换为占位符列表
        Returns:
            占位符列表
        """
        if isinstance(obj, (tuple, list)):
            return list(obj)
        else:
            # 单个占位符
            return [obj]

    def _resolve_inputs(self, inputs: Union[List[Placeholder], type(Ellipsis), AllPlaceholder, PrevPlaceholder]) -> List[Placeholder]:
        """
        解析输入占位符列表，处理特殊符号

        Args:
            inputs: 输入，可以是占位符列表、...、PREV或ALL

        Returns:
            解析后的占位符列表
        """
        # 处理 ... 
        if inputs is ...:
            raise NotImplementedError("... is not implemented yet")

        # 处理 PREV
        if isinstance(inputs, PrevPlaceholder) or (isinstance(inputs, list) and len(inputs) > 0 and isinstance(inputs[0], PrevPlaceholder)):
            if not self.last_outputs:
                raise ValueError("PREV used but there is no previous outputs")
            return self.last_outputs

        # 处理 ALL
        if isinstance(inputs, AllPlaceholder) or (isinstance(inputs, list) and len(inputs) > 0 and isinstance(inputs[0], AllPlaceholder)):
            return [self.placeholders[name] for name in self.var_names]

        if isinstance(inputs, list):
            return inputs

        # 单个占位符
        return [inputs]

    def _resolve_outputs(self, outputs: Union[List[Placeholder], AllPlaceholder], inputs: List[Placeholder]) -> List[Placeholder]:
        """
        解析输出占位符列表，处理特殊符号和缺省规则

        Args:
            outputs: 输出，可以是占位符列表或ALL
            inputs: 对应的输入列表（用于缺省时覆盖）
        Returns:
            解析后的占位符列表
        """
        # 处理 ALL
        if isinstance(outputs, AllPlaceholder) or (isinstance(outputs, list) and len(outputs) > 0 and isinstance(outputs[0], AllPlaceholder)):
            return inputs
        return outputs

    def _execute_map(self, inputs: List[Placeholder], fn: Callable, outputs: List[Placeholder]):
        """
        执行Map模式

        Args:
            inputs: 输入占位符列表
            fn: 处理函数
            outputs: 输出占位符列表

        Raises:
            ValueError: 当输入输出数量不匹配时
        """
        if len(inputs) != len(outputs):
            raise ValueError(f"the parameter number is not match, inputs={len(inputs)}, outputs={len(outputs)}")

        results = []
        for inp in inputs:
            input_value = self.context[inp.name]
            result = fn(input_value)
            results.append(result)

        for placeholder, value in zip(outputs, results):
            self.context[placeholder.name] = value
            # 如果输出占位符不在placeholders中，添加进去
            if placeholder.name not in self.placeholders:
                self.placeholders[placeholder.name] = placeholder

        if self._debug:
            print(f"[MAP] {[p.name for p in inputs]} >> {fn.__name__} >> {[p.name for p in outputs]}")

    def _execute_all(self, inputs: List[Placeholder], fn: Callable, outputs: List[Placeholder]):
        """
        执行All模式

        Args:
            inputs: 输入占位符列表
            fn: 处理函数
            outputs: 输出占位符列表

        Raises:
            ValueError: 当函数返回值数量与输出数量不匹配时
        """
        # 获取所有输入值
        input_values = [self.context[inp.name] for inp in inputs]

        # 调用函数
        result = fn(*input_values)

        # 处理返回值
        if len(outputs) == 1:
            # 单个输出
            results = [result]
        else:
            # 多个输出，需要解包
            if not isinstance(result, (tuple, list)):
                raise ValueError("returned value must be tuple or list when multiple outputs are expected")
            results = list(result)
            if len(results) != len(outputs):
                raise ValueError(f"the parameter number is not match, inputs={len(inputs)}, outputs={len(outputs)}")

        # 更新context
        for placeholder, value in zip(outputs, results):
            self.context[placeholder.name] = value
            if placeholder.name not in self.placeholders:
                self.placeholders[placeholder.name] = placeholder

        if self._debug:
            print(f"[ALL] {[p.name for p in inputs]} >> {fn.__name__} >> {[p.name for p in outputs]}")

    def common(self, *args) -> 'DPP':
        """
        语义：
            _.common([o, p, q] >> fn >> [x, y, z])
            等价于：
            x = fn(o)
            y = fn(p)
            z = fn(q)

        Args:
            *args: 可以是：
                - 单个 CompleteExpression 对象（表达式语法）
                - 三个参数：inputs, fn, outputs（简化语法，支持元组）
                # TODO 如果不是3个也不是单个，尝试解析默认参数

        Returns:
            self，支持链式调用

        Raises:
            ValueError: 当参数格式错误或输入输出数量不匹配时
        """
        # 判断调用方式
        if len(args) == 1 and isinstance(args[0], CompleteExpression):
            # 表达式语法
            expr = args[0]
            inputs = self._resolve_inputs(expr.inputs)
            outputs = self._resolve_outputs(expr.outputs, inputs)
            fn = expr.fn
        elif len(args) == 3:
            inputs_arg, fn, outputs_arg = args
            inputs = self._resolve_inputs(self._convert_to_placeholder_list(inputs_arg))
            outputs = self._resolve_outputs(self._convert_to_placeholder_list(outputs_arg), inputs)
        else:
            raise ValueError("common() needs 1 expression argument, or 3 arguments (inputs, fn, outputs)")

        self._execute_map(inputs, fn, outputs)

        self.last_outputs = outputs

        return self

    def all(self, *args) -> 'DPP':
        """
        语义：
            _.all([o, p, q] >> fn >> [x, y, z])
            等价于：
            x, y, z = fn(o, p, q)

        Args:
            *args: 可以是：
                - 单个 CompleteExpression 对象（表达式语法）
                - 三个参数：inputs, fn, outputs（简化语法，支持元组）
                # todo same as common

        Returns:
            self，支持链式调用
        """
        # 判断调用方式
        if len(args) == 1 and isinstance(args[0], CompleteExpression):
            # 表达式语法
            expr = args[0]
            inputs = self._resolve_inputs(expr.inputs)
            outputs = self._resolve_outputs(expr.outputs, inputs)
            fn = expr.fn
        elif len(args) == 3:
            inputs_arg, fn, outputs_arg = args
            inputs = self._resolve_inputs(self._convert_to_placeholder_list(inputs_arg))
            outputs = self._resolve_outputs(self._convert_to_placeholder_list(outputs_arg), inputs)
        else:
            raise ValueError("all() needs 1 expression argument, or 3 arguments (inputs, fn, outputs)")

        self._execute_all(inputs, fn, outputs)

        self.last_outputs = outputs

        return self

    def branch(self, *args, merge: str = 'last') -> 'DPP':
        """
        并行
            merge: 变量冲突策略
                - 'last': 后写入覆盖
                - 'first': 保留第一次写入
                - 'error': 检测冲突并抛出异常
        Raises:
            ValueError: 当merge='error'且检测到变量冲突时
        """
        all_outputs = []
        written_vars = set()

        branches = []
        if all(isinstance(arg, CompleteExpression) for arg in args):
            branches = [(expr.inputs, expr.fn, expr.outputs) for expr in args]
        elif all(isinstance(arg, tuple) and len(arg) == 3 for arg in args):
            branches = args
        else:
            raise ValueError("branch() must receive all CompleteExpression or all (inputs, fn, outputs) tuples")

        for inputs_arg, fn, outputs_arg in branches:
            inputs = self._resolve_inputs(self._convert_to_placeholder_list(inputs_arg))
            outputs = self._resolve_outputs(self._convert_to_placeholder_list(outputs_arg), inputs)

            # 检查变量冲突
            if merge == 'error':
                for out in outputs:
                    if out.name in written_vars:
                        raise ValueError(f"Variable conflict: {out.name} is written in multiple branches")

            # 执行（默认用Map模式）
            # todo 支持All模式
            if len(inputs) == len(outputs):
                self._execute_map(inputs, fn, outputs)
            else:
                self._execute_all(inputs, fn, outputs)

            for out in outputs:
                if merge == 'first' and out.name in written_vars:
                    continue
                written_vars.add(out.name)

            all_outputs.extend(outputs)

        self.last_outputs = all_outputs

        if self._debug:
            print(f"[BRANCH] {len(branches)} branches executed")

        return self

    def sequence(self, *args) -> 'DPP':
        """
        顺序执行多个步骤


        支持 PREV 符号表示上一步输出。

        Args:
            *args: 多个步骤，可以是：
                - CompleteExpression 对象（表达式语法）
                - (inputs, fn, outputs) 元组（简化语法）

        Returns:
            self，支持链式调用
        """
        steps = []
        if all(isinstance(arg, CompleteExpression) for arg in args):
            # 所有参数都是表达式（旧语法）
            steps = [(expr.inputs, expr.fn, expr.outputs) for expr in args]
        elif all(isinstance(arg, tuple) and len(arg) == 3 for arg in args):
            # 所有参数都是 (inputs, fn, outputs) 元组（新语法）
            steps = args
        else:
            raise ValueError("sequence() must receive all CompleteExpression or all (inputs, fn, outputs) tuples")

        for inputs_arg, fn, outputs_arg in steps:
            inputs = self._resolve_inputs(self._convert_to_placeholder_list(inputs_arg))
            outputs = self._resolve_outputs(self._convert_to_placeholder_list(outputs_arg), inputs)

            if len(inputs) == len(outputs):
                self._execute_map(inputs, fn, outputs)
            else:
                self._execute_all(inputs, fn, outputs)

            self.last_outputs = outputs

        if self._debug:
            print(f"[SEQUENCE] {len(steps)} steps executed")

        return self

    def select(self, *branches, default: Optional[CompleteExpression] = None) -> 'DPP':
        """
        根据条件选择执行分支

        按顺序检查条件，执行第一个满足条件的分支。
        如果所有条件都不满足，执行default分支（如果提供）。

        Args:
            *branches: (expr, condition) 元组列表
                expr: 完整表达式
                condition: 条件函数，接收context对象，返回bool
            default: 默认表达式（可选）

        Example:
            _.select(
                ([x] >> fn1 >> [y], lambda ctx: ctx.x > 0),
                ([x] >> fn2 >> [y], lambda ctx: ctx.x < 0),
                default=[x] >> fn3 >> [y]
            )
        """
        # 创建context对象供条件函数使用
        class Context:
            pass

        ctx = Context()
        for name, value in self.context.items():
            setattr(ctx, name, value)

        executed = False
        for branch in branches:
            if not isinstance(branch, tuple) or len(branch) != 2:
                raise ValueError("select的每个分支必须是 (expr, condition) 元组")

            expr, condition = branch

            if not isinstance(expr, CompleteExpression):
                raise ValueError("分支表达式必须是完整表达式")

            # 检查条件
            if condition(ctx):
                inputs = self._resolve_inputs(expr.inputs)
                outputs = self._resolve_outputs(expr.outputs, inputs)

                if len(inputs) == len(outputs):
                    self._execute_map(inputs, expr.fn, outputs)
                else:
                    self._execute_all(inputs, expr.fn, outputs)

                self.last_outputs = outputs
                executed = True

                if self._debug:
                    print(f"[SELECT] Executed branch with condition {condition}")

                break

        if not executed and default is not None:
            if not isinstance(default, CompleteExpression):
                raise ValueError("default必须是完整表达式")

            inputs = self._resolve_inputs(default.inputs)
            outputs = self._resolve_outputs(default.outputs, inputs)

            if len(inputs) == len(outputs):
                self._execute_map(inputs, default.fn, outputs)
            else:
                self._execute_all(inputs, default.fn, outputs)

            self.last_outputs = outputs

            if self._debug:
                print("[SELECT] Executed default branch")

        return self

    def debug(self, enabled: bool = True) -> 'DPP':
        self._debug = enabled
        return self

    def __getattr__(self, name: str):
        """
        通过属性访问占位符或变量值

        在 with 块内：创建/返回占位符用于定义流程
        在 with 块外：返回变量值用于获取结果

        Args:
            name: 变量名

            with DPP(x=10) as pipe:
                pipe.common(x, double, (pipe.y,))

            print(pipe.y)  # 20
            print(pipe.x)  # 10
        """
        # 避免递归
        if name.startswith('_') or name in ('context', 'placeholders', 'var_names',
                                            'last_outputs', 'common', 'all',
                                            'branch', 'sequence', 'select', 'debug'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        if not getattr(self, '_in_context', False):
            if name in self.context:
                return self.context[name]
            raise AttributeError(f"变量 '{name}' 不存在于 context 中")

        if name in self.placeholders:
            return self.placeholders[name]

        placeholder = Placeholder(name, self)
        self.placeholders[name] = placeholder

        frame = sys._getframe(1)
        frame.f_globals[name] = placeholder

        return placeholder



def create_placeholder(name: str) -> Placeholder:
    return Placeholder(name)


def V(*placeholders) -> PlaceholderList:
    """
    创建占位符列表

    Args:
        *placeholders: 多个占位符对象

    Returns:
        PlaceholderList: 支持 >> 操作符的占位符列表

    Example:
        with DPP(x=1, y=2) as _:
            _.common(V(x, y) >> fn >> V(a, b))
    """
    return PlaceholderList(list(placeholders))



__all__ = [
    'DPP',
    'Placeholder',
    'PlaceholderList',
    'PartialExpression',
    'CompleteExpression',
    'ALL',
    'PREV',
    'V',
    'create_placeholder',
]
