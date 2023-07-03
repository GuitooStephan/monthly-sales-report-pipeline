# Monthly evaluation of pharmaceutical products performance pipeline

This ultimate goal of this project is to develop an ETL pipeline using Airflow that aims at producing monthly performance indicators for various pharmaceutical products.

## Data

The data manipulated in this project represent the sales of pharmaceutical products in various e-commerce websites - lame.com, lyons-evans.com, ross-armstrong.com, wiley-rius.com.

The data is in CSV format with each file representing a table. Although the tables follow a similar structure, the columns names vary between websites. Below is the general structure,

_Order table_
| order_id | order_date |
| ------------- | ------------- |
| 1 | 2023-01-12 |

_Order item table_
| order_id | quantity | ean |
| ------------- | ------------- | ------------- |
| 1 | 4 | 1 |
| 1 | 1 | 2 |

_Product table_
| ean | name | price |
| ------------- | ------------- | ------------- |
| 1 | Lorem ipsum | 18.00 |

Below is the expected result sample,
| order_date | ean | price | quantity | amount |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| 2023-01-01 | 1 | 18.00 | 2 | 36.00 |
| 2023-02-01 | 2 | 5.80 | 1 | 5.80 |

# Solution

## Bottlenecks

- The websites use different column names in their various tables which make aggregation impossible without first normalising the data.
- The data of the previous months are likely to change because of cancellation of orders, so the result produced by this solution should be reflecting these changes.

## Pipelines

1. **Inital** - This pipeline will be executed once with the aim of generating the historical monthly performance indicators.
2. **Monthly** - This pipeline will be executed monthly with the aim of generating the monthly performance indicators of the two previous months. One of the goals is to capture the cancellation of orders which typically occurs within a month from the purchase.

## Implementation

Faced with these constraints, I have decided to implement a first solution based on configuration files (**recommended**) which help normalise the data and produce the desired result. Although this implement is robust and minimise the risk of errors, it is unscalable.

In my attempt of producing a scalable implementation, I have implemented a solution which performs normalisation based on logic. Although this approach might require more tweaking if dealing with a larger population, it is a massive step toward scalability.

Both pipelines were implemented using the first implementation and only the first pipeline was implemented using the second implementation.

# Command

The project was implemented using Airflow, Docker and Postgres. Kindly install Docker and Docker Compose before attempting to run the project locally.

## Setup

```bash
make build
```

## Usage

```bash
make run
```

## Test

```bash
make setup-test-env && make test
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
