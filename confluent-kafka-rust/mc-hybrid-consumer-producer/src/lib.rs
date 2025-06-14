use pyo3::prelude::*;
use pyo3::Bound;
use rand::prelude::*;
use rayon::prelude::*;
use std::time::Instant;

trait IntegrableFunction: Sync + Send {
    fn evaluate(&self, point: &[f64]) -> f64;
}

struct Function1;
impl IntegrableFunction for Function1 {
    fn evaluate(&self, point: &[f64]) -> f64 {
        if point.len() != 2 {
            panic!("Function1 expects 2 dimensions.");
        }
        let x = point[0];
        let y = point[1];
        (-(x * x + y * y)).exp()
    }
}

struct Function2;
impl IntegrableFunction for Function2 {
    fn evaluate(&self, point: &[f64]) -> f64 {
        if point.len() != 3 {
            panic!("Function2 expects 3 dimensions.");
        }
        let x = point[0];
        let y = point[1];
        let z = point[2];
        x.sin() * y.cos() + z * z
    }
}

#[pyfunction]
fn monte_carlo_integrate_rs(
    py: Python,
    function_id: String,
    dimensions: usize,
    integration_bounds: Vec<Vec<f64>>,
    num_samples: u64,
) -> PyResult<(f64, f64, f64)> {
    let start_time = Instant::now();

    let func: Box<dyn IntegrableFunction> = match function_id.as_str() {
        "gaussian_2d" => Box::new(Function1),
        "sin_cos_poly_3d" => Box::new(Function2),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown function_id: {}",
                function_id
            )))
        }
    };

    if dimensions != integration_bounds.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Dimensions ({}) do not match number of integration_bounds ({})",
            dimensions,
            integration_bounds.len()
        )));
    }

    for bounds_pair in &integration_bounds {
        if bounds_pair.len() != 2 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Each integration bound must be a list of two elements [min, max]".to_string(),
            ));
        }
    }

    let domain_volume: f64 = integration_bounds
        .iter()
        .map(|pair| pair[1] - pair[0])
        .product();

    let (sum_of_values, sum_of_squares) = py.allow_threads(move || {
        (0..num_samples)
            .into_par_iter()
            .map_init(
                || thread_rng(),
                |rng, _| {
                    let mut point = vec![0.0; dimensions];
                    for i in 0..dimensions {
                        let min = integration_bounds[i][0];
                        let max = integration_bounds[i][1];
                        point[i] = rng.gen_range(min..max);
                    }
                    func.evaluate(&point)
                },
            )
            .fold(
                || (0.0, 0.0),
                |(current_sum_values, current_sum_squares), value| {
                    (
                        current_sum_values + value,
                        current_sum_squares + value * value,
                    )
                },
            )
            .reduce(
                || (0.0, 0.0),
                |(a_sum, a_sq_sum), (b_sum, b_sq_sum)| (a_sum + b_sum, a_sq_sum + b_sq_sum),
            )
    });

    let avg_value = sum_of_values / num_samples as f64;
    let estimated_integral = domain_volume * avg_value;

    let variance = (sum_of_squares / num_samples as f64) - (avg_value * avg_value);
    let std_dev = if variance >= 0.0 {
        variance.sqrt()
    } else {
        0.0
    };
    let error_estimate = std_dev / (num_samples as f64).sqrt();

    let duration = start_time.elapsed();
    let computation_time_ms = duration.as_secs_f64() * 1000.0;

    Ok((estimated_integral, error_estimate, computation_time_ms))
}

#[pymodule]
fn mc_hybrid_consumer_producer(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(monte_carlo_integrate_rs, &m)?)?;
    Ok(())
}
