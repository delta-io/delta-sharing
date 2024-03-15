import delta_kernel_python
import pyarrow as pa
import sys

if len(sys.argv) != 2:
    print("Usage %s [table_path]" % sys.argv[0])
    sys.exit(-1)

table_path = sys.argv[1]
print(f"Reading: {table_path}")

interface = delta_kernel_python.PythonInterface(table_path)
table = delta_kernel_python.Table(table_path)
snapshot = table.snapshot(interface)
print("Table Version %i" % snapshot.version())

scan = delta_kernel_python.ScanBuilder(snapshot).build()
table = pa.Table.from_batches(scan.execute(interface))
print(table.to_pandas())


