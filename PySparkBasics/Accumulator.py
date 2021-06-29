""" Accumulators
Accumulators are write-only and initialize once variables where only tasks that are running on workers are
allowed to update and updates from the workers get propagated automatically to the driver program. But, only the
driver program is allowed to access the Accumulator variable using the value property.

We can create Accumulators in PySpark for primitive types int and float. Users can also create Accumulators for
custom types using AccumulatorParam class of PySpark.
"""


