"""Shared PySpark code for the retail reviews lakehouse.

The jobs in jobs/ are thin entry points. Everything they do lives here as
plain functions, so the same code runs on a laptop, in the tests and on Glue.
"""
