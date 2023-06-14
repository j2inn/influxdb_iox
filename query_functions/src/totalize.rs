use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::cast::as_float64_array;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature,
    Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;
use once_cell::sync::Lazy;
use std::sync::Arc;

pub(crate) const TOTALIZE_UDF_NAME: &str = "totalize";

pub(crate) static TOTALIZE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    let signatures = vec![TypeSignature::Exact(vec![ArrowDataType::Float64])];

    Arc::new(ScalarUDF::new(
        TOTALIZE_UDF_NAME,
        &Signature::one_of(signatures, Volatility::Volatile),
        &return_type_fn,
        &totalize(),
    ))
});

fn totalize() -> ScalarFunctionImplementation {
    make_scalar_function(|args: &[ArrayRef]| {
        let array = as_float64_array(&args[0]).expect("cast failed");
        let iter = array
            .iter()
            // Combine two iterators where the first iterator contains all the entries, and the second one is shifted
            // by one position:
            // 1st iterator (next values): [val1, val2, ... valN]
            // 2nd iterator (current values): [None, val1, val2, ... valN]
            .zip([None].into_iter().chain(array.iter()))
            // Then the delta between each corresponding value is calculated when both values are different
            // from None.
            .map(|(next, current)| next.zip(current).map(|(n, c)| n - c));

        Ok(Arc::new(Float64Array::from_iter(iter)) as ArrayRef)
    })
}
