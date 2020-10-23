from pyspark.sql.functions import col

"""Toy join function to showcase spark functions."""

def join_dataframes(left, right, columns_left, columns_right, join_type='inner'):
    if len(columns_left) == len(columns_right) and len(columns_left) > 0:
        cond = [col(left_col) == col(right_col) for (left_col, right_col) in zip(columns_left, columns_right)]
        return left.join(right, cond, join_type)
    raise Error('Columns parameters don\'t match or empty')
