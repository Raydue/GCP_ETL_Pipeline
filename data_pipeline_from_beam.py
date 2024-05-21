import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery


parser = argparse.ArgumentParser()

parser.add_argument(
    '--input',
    dest ='input',  # input file path
    required = True, # The user much type --input ""
    help = 'Input file to process.',
    )
#pipeline_args holds env setting. ike job name, temp file location...
#path_args holds input file loc
path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input

options = PipelineOptions(pipeline_args) #convert env set to pipeline style obj.

p = beam.Pipeline(options= options) # plug in beam pipeline.

        ####specific data cleaning.####

def remove_last_colon(row):		# OXJY167254JK,11-09-2020,8:11:21,854A854,Chow M?ein:,65,Cash,Sadabahar,Delivered,5,Awesome experience
    cols = row.split(',')		# [(OXJY167254JK) (11-11-2020) (8:11:21) (854A854) (Chow M?ein:) (65) (Cash) ....]
    item = str(cols[4])			# item = Chow M?ein:
    
    if item.endswith(':'):
        cols[4] = item[:-1]		# cols[4] = Chow M?ein

    return ','.join(cols)		# OXJY167254JK,11-11-2020,8:11:21,854A854,Chow M?ein,65,Cash,Sadabahar,Delivered,5,Awesome experience
def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',')			# col = [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col) #remove "?%&" with ''.
        ret = ret + clean_col + ','			# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1]						# oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret

cleaned_data = (
    p
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines = 1)
    | beam.Map(remove_last_colon)   #transformation. remove ":" on the items column
    | beam.Map(lambda row : row.lower())
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row+',1')		# add a '1' in the last column, user for takeing count of every rows.
)

delivered_order = (
    cleaned_data
    | 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() == "delivered")   #filter each row if the 9th col is delivered or not.
)

other_order = (
    cleaned_data
    | 'undelivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != "delivered") #filter each row if the 9th col is delivered or not.
)

def print_row(row):
    print(row)

(cleaned_data
 | 'count total' >> beam.combiners.Count.Globally()
 | 'total map' >> beam.Map(lambda x: 'Total count:'+str(x))
 | 'print total' >> beam.Map(print_row)
 )

(
    delivered_order
    | 'count delivered' >> beam.combiners.Count.Globally() # Counts all elements globally    #920
    | 'delivered map' >> beam.Map(lambda x : 'Delivered Count:' + str(x)) # Formats the count into a string  #Total Count: 920
    | 'print delivered count' >> beam.Map(print_row) # count each element, for debugging.
)

(other_order
 | 'count others' >> beam.combiners.Count.Globally()
 | 'other map' >> beam.Map(lambda x: 'Others count:'+str(x))
 | 'print undelivered' >> beam.Map(print_row)
 )

# BigQuery #for cloud shell
client = bigquery.Client()

dataset_id = "{}.dataset_food_orders".format(client.project)

try:
	client.get_dataset(dataset_id)
	
except:     #if dataset is not existed, create it.
	dataset = bigquery.Dataset(dataset_id)  #

	dataset.location = "US"
	dataset.description = "dataset for food orders"

	dataset_ref = client.create_dataset(dataset, timeout=30)  # Make an API request.
#the schema for table.     
table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'

#convert pcollection into json string
def to_json(csv_str):
    fields = csv_str.split(',')
    json_format = {
                    "customer_id":fields[0],
                    "date":fields[1],
                    "timestamp":fields[2],
                    "order_id":fields[3],
                    "items":fields[4],
                    "amount":fields[5],
                    "mode":fields[6],
                    "restaurant":fields[7],
                    "status":fields[8],
                    "ratings":fields[9],
                    "feedback":fields[10],
                    "new_col":fields[11]
    }
    return json_format
#project-id:dataset_id.table_id
delivered_table_spec = 'my-baby-project-422109:dataset_food_orders.delivered_orders'
#project-id:dataset_id.table_id
other_table_spec = 'my-baby-project-422109:dataset_food_orders.other_status_orders'
(
    delivered_order
    | 'delivered to json' >> beam.Map(to_json)
    | 'write_delivered' >> beam.io.WriteToBigQuery(
        delivered_table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # CREATE_IF_NEEDED means this won't throw already exist error when the pipeline is rerun. It will create only if the table is not exitst.
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # this allows daily runs append into the table.
        additional_bq_parameters={'timePartitioning': {'type' : 'DAY'}} #partition by date, for daily runs.
    )
)

(
    other_order
    | 'others to json' >> beam.Map(to_json)
    | 'write_others' >> beam.io.WriteToBigQuery(
        other_table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # CREATE_IF_NEEDED means this won't throw already exist error when the pipeline is rerun. It will create only if the table is not exitst.
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # this allows daily runs append into the table.
        additional_bq_parameters={'timePartitioning': {'type' : 'DAY'}}
    )
)

#just want to see the job is successful or not. The vanilla p.run returns nothing. 
from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print("Job success!!!")
else:
    print("Error running beam pipeline.")


#create the daily view
view_name = "daily_food_orders"
dataset_ref = client.dataset('dataset_food_orders')
view_ref = dataset_ref.table(view_name)
view_to_create = bigquery.Table(view_ref)

view_to_create.view_query = """
SELECT *
FROM `my-baby-project-422109.dataset_food_orders`
WHERE _PARTITIONDATE = DATE(current_date())
""" #select today's data.
view_to_create.view_use_legacy_sql = False
try:
    client.create_table(view_to_create)
except:
    print("view already exists.")