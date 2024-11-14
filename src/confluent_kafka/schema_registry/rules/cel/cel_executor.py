# Copyright 2024 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import celpy

from threading import Lock
from typing import Any, Optional

from celpy.celtypes import BoolType

from confluent_kafka.schema_registry import RuleKind, Schema
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_presence import \
    InterpretedRunner
from confluent_kafka.schema_registry.rules.cel.constraints import _msg_to_cel
from confluent_kafka.schema_registry.rules.cel.extra_func import EXTRA_FUNCS
from confluent_kafka.schema_registry.serde import RuleExecutor, RuleContext


class CelExecutor(RuleExecutor):

    def __init__(self):
        self._env = celpy.Environment(runner_class=InterpretedRunner)
        self._funcs = EXTRA_FUNCS
        self._cache = _CelCache()

    def type(self) -> str:
        return "CEL"

    def transform(self, ctx: RuleContext, message: Any) -> Any:
        args = { "message": _msg_to_cel(message) }
        return self.execute(ctx, message, args)

    def execute(self, ctx: RuleContext, message: Any, args: Any) -> Any:
        expr = ctx.rule.expr
        index = expr.index(";")
        if index >= 0:
            guard = expr[:index]
            if len(guard.strip()) > 0:
                guard_result = self.execute_rule(ctx, guard, args)
                if not guard_result:
                    if ctx.rule.kind == RuleKind.CONDITION:
                        return True
                    return message
            expr = expr[index+1:]

        return self.execute_rule(ctx, expr, args)

    def execute_rule(self, ctx: RuleContext, expr: str, args: Any) -> Any:
        schema = ctx.target.schema
        script_type = ctx.target.schema.schema_type
        prog = self._cache.get_program(expr, script_type, schema)
        if prog is None:
            ast = self._env.compile(expr)
            prog = self._env.program(ast, functions=self._funcs)
            self._cache.set(expr, script_type, schema, prog)
        result = prog.evaluate(args)
        if isinstance(result, BoolType):
            return bool(result)
        return result

    @staticmethod
    def register():
        RuleRegistry.register_rule_executor(CelExecutor())
        RuleRegistry.register_rule_executor(CelFieldExecutor())


class _CelCache(object):
    def __init__(self):
        self.lock = Lock()
        self.programs = {}

    def set(self, expr: str, script_type: str, schema: Schema, prog: celpy.Runner):
        with self.lock:
            self.programs[(expr, script_type, schema)] = prog

    def get_program(self, expr: str, script_type: str, schema: Schema) -> Optional[celpy.Runner]:
        with self.lock:
            return self.programs.get((expr, script_type, schema), None)

    def clear(self):
        with self.lock:
            self.programs.clear()
