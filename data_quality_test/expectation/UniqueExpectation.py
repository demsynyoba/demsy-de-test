from data_quality_test.expectation.Expectation import Expectation


class UniqueExpectation(Expectation):
    def __init__(self, column, metric, add_info={}):
        super().__init__(column, metric, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_unique(
            column=self.column, meta={"metric": self.metric}
        )
