import fastavro


with open('output.avro', 'rb') as f:
    for record in fastavro.reader(f):
        print(record)