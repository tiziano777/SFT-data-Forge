def set_const_value(val):
    return val

def upper_case(val):
    return val.upper()

mapping = {
    "context": [set_const_value, "You are a bot"],
    "shout": [upper_case, "hello"]
}

for k, rule in mapping.items():
    func, *args = rule
    result = func(*args)
    print(k, "->", result)