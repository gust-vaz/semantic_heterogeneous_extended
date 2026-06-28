import itertools

records = [200000]
versions = [5,10,15,20]
fields = [20]
repetitions = 30
method='operations_first'
evolution_fields=[2]
operations = [500]
domain = [40]
update_percent=[0,0.05,0.5,0.95,1]
mode=['preprocess','rewrite']

arguments = itertools.product(*[records,versions,fields,evolution_fields,operations,domain,update_percent,mode])

out = list()
for arg_combination in arguments:
    output = f'{arg_combination[0]}records_{arg_combination[1]}versions_{arg_combination[2]}fields_{arg_combination[3]}evfields_{arg_combination[4]}ops_{arg_combination[5]}domain_{arg_combination[6]*100}update_{arg_combination[7]}.csv'
    out.append(f'python3 simulations.py --records={arg_combination[0]} --versions={arg_combination[1]} --fields={arg_combination[2]} --domain={arg_combination[5]} --repetitions={repetitions} --operations={arg_combination[4]} --destination="{output}" --method={method} --update_percent={arg_combination[6]} --evolution_fields={arg_combination[3]} --mode={arg_combination[7]}; ')

with open('simulations_batch.sh','w') as file:
    file.writelines(out)