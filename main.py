from neo4j import GraphDatabase
import pandas, traceback

data = pandas.read_excel("sample_input.xlsx", header=0)


datasets = data['dataset_name']
terms_cls = data[['term','class']]

#print(type(terms_cls))

conn = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j","password"))


session = conn.session()

#print(session)
#res = session.run("CREATE CONSTRAINT unique_dataset ON (dataset:dataset_name) ASSERT dataset.name IS UNIQUE")
#res = session.run("CREATE CONSTRAINT unique_terms ON (term:term) ASSERT term.name IS UNIQUE")
#res = session.run("CREATE CONSTRAINT unique_domains ON (domain:domain) ASSERT domain.domain_name IS UNIQUE")
#print(res)


#Creating first the datasets with unique constraint
for row in datasets:
    q = f"CREATE (:dataset_name {{name: '{row}'}})"
    try:
        session.run(q)
    except:
        continue

#Creating unique terms along with their classes set as properties

for index, row in terms_cls.iterrows():
    term_name = row['term']
    cls = row['class']
    q = f"CREATE (:term {{name: '{term_name}', class: '{cls}'}})"
    #print(q)
    try:
        session.run(q)
    except:
        print(traceback.print_exc())
    # print(row['term'], row['class'])


#Creating the relationship between term & dataset, and relationship telling about proability of that term in the dataset

for index, row in data.iterrows():
    dataset_name = row['dataset_name']
    term = row['term']
    probability = row['probability']
    q = f'''
        MATCH (t:term),(d:dataset_name)
        WHERE t.name = '{term}' and d.name = '{dataset_name}'
        MERGE (t)-[:PRESENT_IN]->(d)'''
    
    try:
        session.run(q)
    except:
        print(traceback.print_exc())

    
    q = f'''MATCH (t:term),(d:dataset_name)
        WHERE t.name = '{term}' and d.name = '{dataset_name}'
        MERGE (t)-[:TERM_PROBABILITY {{probability: '{probability}'}}]->(d)'''
    
    try:
        session.run(q)
    except:
        print(traceback.print_exc())

    #print(row['term'], row['class'])


for index, row in data.iterrows():
    domains = row['domain']
    dset_name = row['dataset_name']
    domains = domains.split(',')
    for domain in domains:
        q = f"CREATE (:domain {{domain_name: '{domain.strip()}'}})"

        #print(q)
        try:
            session.run(q)
        except:
            print(traceback.print_exc())
    
    for obj in range(len(domains)-1,-1,-1):
        if not obj == 0:
            q = f'''
                MATCH (d1:domain),(d2:domain)
                WHERE d1.domain_name = '{domains[obj].strip()}' and d2.domain_name = '{domains[obj-1].strip()}'
                MERGE (d1)-[:SUB_DOMAIN_OF]->(d2)'''
            
            try:
                session.run(q)
            except:
                print(traceback.print_exc())        

       
        q = f'''
            MATCH (domain:domain),(dset:dataset_name)
            WHERE domain.domain_name = '{domains[len(domains)-1].strip()}' and dset.name = '{dset_name}'
            MERGE (domain)-[:PART_OF]->(dset)'''
        
        try:
            session.run(q)
        except:
            print(traceback.print_exc()) 

session.close()