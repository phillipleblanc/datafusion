// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::DataType;
use datafusion_common::{plan_datafusion_err, DFSchema, Result};
use crate::{Case, ExprSchemable};
use super::binary::comparison_coercion;
use itertools::{Itertools as _};

/// Attempts to coerce the types of `list_types` to be comparable with the
/// `expr_type`.
/// Returns the common data type for `expr_type` and `list_types`
pub fn get_coerce_type_for_list(
    expr_type: &DataType,
    list_types: &[DataType],
) -> Option<DataType> {
    list_types
        .iter()
        .try_fold(expr_type.clone(), |left_type, right_type| {
            comparison_coercion(&left_type, right_type)
        })
}

pub fn get_coerce_types_for_case_expression(case: &Case, schema: &DFSchema) -> Result<(Result<Option<DataType>>, Result<DataType>)> {
    // prepare types
    let case_type = case
        .expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;
    let then_types = case
        .when_then_expr
        .iter()
        .map(|(_when, then)| then.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let else_type = case
        .else_expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;

    // find common coercible types
    let case_when_coerce_type = case_type
        .as_ref()
        .map(|case_type| {
            let when_types = case
                .when_then_expr
                .iter()
                .map(|(when, _then)| when.get_type(schema))
                .collect::<Result<Vec<_>>>()?;
            let coerced_type =
                get_coerce_type_for_case_expression(&when_types, Some(case_type));
            coerced_type.ok_or_else(|| {
                plan_datafusion_err!(
                    "Failed to coerce case ({case_type}) and when ({}) \
                     to common types in CASE WHEN expression",
                    when_types.iter().join(", ")
                )
            })
        })
        .transpose();
    let then_else_coerce_type =
        get_coerce_type_for_case_expression(&then_types, else_type.as_ref()).ok_or_else(
            || {
                if let Some(else_type) = else_type {
                    plan_datafusion_err!(
                        "Failed to coerce then ({}) and else ({else_type}) \
                         to common types in CASE WHEN expression",
                        then_types.iter().join(", ")
                    )
                } else {
                    plan_datafusion_err!(
                        "Failed to coerce then ({}) and else (None) \
                         to common types in CASE WHEN expression",
                        then_types.iter().join(", ")
                    )
                }
            },
        );
    Ok((case_when_coerce_type, then_else_coerce_type))
}

/// Find a common coerceable type for all `when_or_then_types` as well
/// and the `case_or_else_type`, if specified.
/// Returns the common data type for `when_or_then_types` and `case_or_else_type`
pub fn get_coerce_type_for_case_expression(
    when_or_then_types: &[DataType],
    case_or_else_type: Option<&DataType>,
) -> Option<DataType> {
    let case_or_else_type = match case_or_else_type {
        None => when_or_then_types[0].clone(),
        Some(data_type) => data_type.clone(),
    };
    when_or_then_types
        .iter()
        .try_fold(case_or_else_type, |left_type, right_type| {
            // TODO: now just use the `equal` coercion rule for case when. If find the issue, and
            // refactor again.
            comparison_coercion(&left_type, right_type)
        })
}
