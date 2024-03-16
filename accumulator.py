from pyspark.accumulators import AccumulatorParam

# Definisci una classe per l'accumulator
class ListAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return initialValue

    def addInPlace(self, v1, v2):
        return v1 + v2