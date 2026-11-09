import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        value = int(value)

    except ValueError:
        sys.stderr.write("Invalid mapper output encountered\n")
        continue

    if key == current_key:
        current_total += value

    else:
        if current_key is not None:
            print(f"{current_key}\t{current_total}")

        current_key = key
        current_total = value

# Emit the final key after reaching end of input
if current_key is not None:
    print(f"{current_key}\t{current_total}")
