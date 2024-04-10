from data_quality_test.expectation.Expectation import Expectation


class ValuesInListExpectation(Expectation):
    def __init__(self, column, metric, add_info={}):
        super().__init__(column, metric, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_in_set(
            column=self.column,
            value_set=self.add_info["value_set"],
            meta={"metric": self.metric},
        )
