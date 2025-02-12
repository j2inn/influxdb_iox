use influxdb_iox_client::connection::Connection;

use crate::commands::namespace::Result;
use influxdb_iox_client::namespace::generated_types::ServiceProtectionLimits;

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to to be created
    #[clap(action)]
    namespace: String,

    /// Num of hours of the retention period of this namespace.
    /// If not specified, an infinite retention period will be used.
    #[clap(
        action,
        long = "retention-hours",
        short = 'r',
        env = "INFLUXDB_IOX_NAMESPACE_RETENTION_HOURS",
        default_value = "0"
    )]
    retention_hours: u32,

    #[clap(flatten)]
    service_protection_limits: ServiceProtectionLimitsArgs,
}

#[derive(Debug, clap::Args)]
pub struct ServiceProtectionLimitsArgs {
    /// The maximum number of tables to allow for this namespace
    #[clap(action, long = "max-tables", short = 't')]
    max_tables: Option<i32>,

    /// The maximum number of columns to allow per table for this namespace
    #[clap(action, long = "max-columns-per-table", short = 'c')]
    max_columns_per_table: Option<i32>,
}

impl From<ServiceProtectionLimitsArgs> for Option<ServiceProtectionLimits> {
    fn from(value: ServiceProtectionLimitsArgs) -> Self {
        let ServiceProtectionLimitsArgs {
            max_tables,
            max_columns_per_table,
        } = value;
        if max_tables.is_none() && max_columns_per_table.is_none() {
            return None;
        }
        Some(ServiceProtectionLimits {
            max_tables,
            max_columns_per_table,
        })
    }
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config {
        namespace,
        retention_hours,
        service_protection_limits,
    } = config;

    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    // retention_hours = 0 means infinite retention. Make it None/Null in the request.
    let retention: Option<i64> = if retention_hours == 0 {
        None
    } else {
        // we take retention from the user in hours, for ease of use, but it's stored as nanoseconds
        // internally
        Some(retention_hours as i64 * 60 * 60 * 1_000_000_000)
    };
    let namespace = client
        .create_namespace(
            &namespace,
            retention,
            service_protection_limits.into(),
            None,
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    Ok(())
}
