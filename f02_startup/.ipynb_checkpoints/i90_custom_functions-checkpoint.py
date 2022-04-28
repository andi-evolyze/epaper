
# Importiere Dependencies


from json import loads as json_loads
from json import dumps as json_dumps

from shopify import GraphQL as shopify_GraphQL

from os.path import getsize as os_path_getsize
from os import getenv as os_getenv

from requests import post as requests_post

from pandas import read_json as pd_read_json
from pandas import json_normalize as pd_json_normalize
from pandas import notnull as pd_notnull
from pandas import to_datetime as pd_to_datetime
from pandas import Timestamp as pd_timestamp
from pandas import DataFrame
from pandas import read_sql as pd_read_sql


from time import sleep as time_sleep
 
    

from httplib2 import Http as httplib2_Http

from yagmail import SMTP as yagmail_SMTP



from sqlalchemy import create_engine as sqlalchemy_create_engine

from sqlalchemy.types import INTEGER as sqlalchemy_types_INTEGER
from sqlalchemy.types import VARCHAR as sqlalchemy_types_VARCHAR
from sqlalchemy.types import DateTime as sqlalchemy_types_DateTime
from sqlalchemy.types import Boolean as sqlalchemy_types_Boolean
from sqlalchemy.types import Float as sqlalchemy_types_Float

from sshtunnel import SSHTunnelForwarder as sshtunnel_SSHTunnelForwarder
from sshtunnel import SSH_TIMEOUT as sshtunnel_SSH_TIMEOUT
from sshtunnel import TUNNEL_TIMEOUT as sshtunnel_TUNNEL_TIMEOUT

 
# temp


from pandas import read_pickle as pd_read_pickle
from pandas import concat as pd_concat
#from pandas import reset_index as pd_reset_index
#from pandas import to_pickle as pd_to_pickle



#############

def shopify_stage_upload(file_url):
    
    # Wird nur ausgeführt, wenn Datei nicht leer ist    
    if os_path_getsize(file_url) > 0:
    
    
            stage= json_loads(shopify_GraphQL().execute(

                        '''

                        mutation {
                          stagedUploadsCreate(input:{
                            resource: BULK_MUTATION_VARIABLES,
                            filename: "bulk_op_vars",
                            mimeType: "text/jsonl",
                            httpMethod: POST
                          }){
                            userErrors{
                              field,
                              message
                            },
                            stagedTargets{
                              url,
                              resourceUrl,
                              parameters {
                                name,
                                value
                              }
                            }
                          }
                        }
                        '''
                                             ))


            stage2= stage["data"]["stagedUploadsCreate"]["stagedTargets"][0]["parameters"]

            url = stage["data"]["stagedUploadsCreate"]["stagedTargets"][0]["url"]



            headers = {    
                 stage2[0]["name"] : stage2[0]["value"],
                 stage2[5]["name"] : stage2[5]["value"],
                 stage2[6]["name"] : stage2[6]["value"],
                 stage2[7]["name"] : stage2[7]["value"],
                 stage2[8]["name"] : stage2[8]["value"],
                 stage2[4]["name"] : stage2[4]["value"],
                 stage2[3]["name"] : stage2[3]["value"],
                 stage2[1]["name"] : stage2[1]["value"],
                 stage2[2]["name"] : stage2[2]["value"],
                    "file":  open(file_url, 'rb')

                }

            r = requests_post(url,files= headers)
            print(r.reason)
            return stage2


def shopify_variant_update(file_url):
    
    # Wird nur ausgeführt, wenn Datei nicht leer ist
    if os_path_getsize(file_url) > 0:    
    
            stage2 = shopify_stage_upload(file_url)

            stage3 = json_loads(shopify_GraphQL().execute(
                        '''

                        mutation {
                          bulkOperationRunMutation(


                        mutation: """

                        mutation ($input: ProductVariantInput!) {
                          productVariantUpdate(input: $input) {
                            userErrors {
                              message
                              field
                            }
                          }
                        }                                  

                            """ ,
                            stagedUploadPath: "''' + stage2[0]["value"] +   '''"                                

                          ) {
                            bulkOperation {
                              id
                              status
                            }
                            userErrors {
                              field
                              message
                            }
                          }
                        }
                        '''
            ))
            #print(stage3)
            time_sleep(1)
            
            # Upload in Datenbank / pkl       

            temp = pd_json_normalize(pd_read_json(file_url, lines=True)["input"])
            temp["created"] = pd_timestamp.now(tz='Europe/Zurich').strftime('%Y-%m-%d %H:%M:%S')

            temp = temp.rename(columns = { 'id':"variant_gid" })  
            temp["s_topic"] = str(stage3)
            
            if "metafields" in temp.columns:
                    temp[['metafield_gid', 'namespace', 'key', 'value', 'valueType']] = pd_json_normalize(temp["metafields"].str[0])
                    temp = temp.drop(columns= ["metafields"])
            
            temp =pd_concat(
             [
                 pd_read_pickle("f05_bulk/variant_update.pkl")
                 ,temp
             ]   
            ).reset_index(drop=True)
            temp.to_pickle("f05_bulk/variant_update.pkl")    
     
    else:
        print("Datei ist leer")            
            
            

###############################################    
    
def shopify_product_update(file_url):

    # Wird nur ausgeführt, wenn Datei nicht leer ist
    if os_path_getsize(file_url) > 0:     


            stage2 = shopify_stage_upload(file_url) 

            stage3= json_loads(shopify_GraphQL().execute(

                        '''

                        mutation {
                          bulkOperationRunMutation(


                        mutation: """

                        mutation ($input: ProductInput!) {
                          productUpdate(input: $input) {
                            userErrors {
                              message
                              field
                            }
                          }
                        }                                  

                            """ ,
                            stagedUploadPath: "''' + stage2[0]["value"] +   '''"                                

                          ) {
                            bulkOperation {
                              id
                              status
                            }
                            userErrors {
                              field
                              message
                            }
                          }
                        }
                        '''

            ))
            #print(stage3)
            time_sleep(1)
            
            # Upload in Datenbank / pkl       
    
            temp = pd_json_normalize(pd_read_json(file_url, lines=True)["input"])
            temp["created"] = pd_timestamp.now(tz='Europe/Zurich').strftime('%Y-%m-%d %H:%M:%S')

            temp = temp.rename(columns = { 'id':"product_gid" }) 
            temp["s_topic"] = str(stage3)

            if "metafields" in temp.columns:
                    temp[['metafield_gid', 'namespace', 'key', 'value', 'valueType']] = pd_json_normalize(temp["metafields"].str[0])
                    temp = temp.drop(columns= ["metafields"])
            
            temp =pd_concat(
             [
                 pd_read_pickle("f05_bulk/product_update.pkl")
                 ,temp
             ]   
            ).reset_index(drop=True)
            temp.to_pickle("f05_bulk/product_update.pkl")   
            
            

    else:
        print("Datei ist leer")
            
#####################


def status_bulk(wait=False,sleep_time=60): 
    
    time_sleep(1)
    query = '''

                    query {
                      currentBulkOperation {
                        id    status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        partialDataUrl
                      }
                    }

            '''

    status = json_loads(shopify_GraphQL().execute(query))
    
    if wait == True:

            while status["data"]["currentBulkOperation"]["status"] == 'RUNNING':
                    print("Waiting "+ str(sleep_time) + " seconds")
                    time_sleep(sleep_time)
                    status = json_loads(shopify_GraphQL().execute(query))
                    


    if status["data"]["currentBulkOperation"]["status"] == 'COMPLETED':
            print("Done")
            #print(status)
            return pd_read_json(status["data"]["currentBulkOperation"]["url"], lines=True)
        
    if status["data"]["currentBulkOperation"]["status"] == 'RUNNING':
            print("Still Running")     
        
    else:
            print("Error")
            print(status)

            
            
            
####################            
            
def status_mutation(wait=False,sleep_time=30): 
    
    query = '''

                    query {
                      currentBulkOperation(type: MUTATION) {
                        id    status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        partialDataUrl
                      }
                    }

            '''

    status = json_loads(shopify_GraphQL().execute( query ))
    
    if wait == True:

            while status["data"]["currentBulkOperation"]["status"] == 'RUNNING':
                    print("Waiting "+ str(sleep_time) + " seconds")
                    time_sleep(sleep_time)
                    status = json_loads(shopify_GraphQL().execute(query))
                      
    
    
    if status["data"]["currentBulkOperation"]["status"] == 'COMPLETED':
            
            print( "Done")
            print("Created at:" +status["data"]["currentBulkOperation"]["createdAt"])
            status_jsonl = pd_read_json(status["data"]["currentBulkOperation"]["url"], lines=True)
            status_jsonl["data"] = pd_json_normalize(status_jsonl["data"])
            
            temp = status_jsonl[status_jsonl["data"].str.len()>0].copy()
            temp["topic"] = "mutation"
            
            temp =pd_concat(
             [
                 pd_read_pickle("f05_bulk/mutation_error.pkl")
                 ,temp
             ]   
            ).reset_index(drop=True)

            temp.to_pickle("f05_bulk/mutation_error.pkl")   
            
            
            return status_jsonl[status_jsonl["data"].str.len()>0].copy()           
          
        
        
        
            
    elif status["data"]["currentBulkOperation"]["status"] == 'RUNNING':
            
            print( "Still Running")       
            
    else:
            print(status["data"]["currentBulkOperation"]["status"])
            print(status)


######################            
            
def variant_ref(data): 
    data= data[["variant_gid","meta_v_variant_ref_meta_gid","meta_v_variant_ref_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)
     
    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_variant_ref_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "variant_ref",
                                "value":    data.loc[item]["meta_v_variant_ref_value_neu"],
                                "valueType": "STRING"
                        }]
                }}
            ) + "\n")  
            
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")            
            
            
#################


def variant_ref_2(data): 
    

    data= data[["variant_gid","meta_v_variant_ref_2_meta_gid","meta_v_variant_ref_2_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)

    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_variant_ref_2_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "variant_ref_2",
                                "value":    data.loc[item]["meta_v_variant_ref_2_value_neu"],
                                "valueType": "STRING"
                        }]
                    }}
            ) + "\n")
            
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")    
            
            
##################

def product_ref(data): 
    
    
    data= data[["product_gid","meta_p_product_ref_meta_gid","meta_p_product_ref_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)

    with open("f05_bulk/product.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["product_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_p_product_ref_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "product_ref",
                                "value": data.loc[item]["meta_p_product_ref_value_neu"],
                                "valueType": "STRING"
                        }]
                    }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")
        
        
########

def product_ref_2(data): 
    
    data= data[["product_gid","meta_p_product_ref_2_meta_gid","meta_p_product_ref_2_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)
   
    with open("f05_bulk/product.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["product_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_p_product_ref_2_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "product_ref_2",
                                "value": data.loc[item]["meta_p_product_ref_2_value_neu"],
                                "valueType": "STRING"
                        }]
                    }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")
        
      
           
##################

def product_stock_off(data): 
    
    
    data= data[["product_gid","meta_p_stock_off_meta_gid","meta_p_stock_off_value_neu"]].copy()
    data=data.drop_duplicates()
    data=data.where(pd_notnull(data), None)

    with open("f05_bulk/product.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["product_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_p_stock_off_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "stock_off",
                                "value": data.loc[item]["meta_p_stock_off_value_neu"]
                                #,"valueType": "STRING"
                        }]
                    }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")
        
           
#######
            
def variant_barcode(data): 
    
    
    data= data[["variant_gid","barcode_neu"]].copy()
    data=data.where(pd_notnull(data), None)
   
    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "barcode": data.loc[item]["barcode_neu"]
                }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")                    
                    
                    
                    
                    
#####

def variant_delivery_date(data): 
    
    data= data[["variant_gid","meta_v_delivery_date_meta_gid","meta_v_delivery_date_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)

      
    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_delivery_date_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "delivery_date",
                                "value":   data.loc[item]["meta_v_delivery_date_value_neu"],
                                "valueType": "STRING"
                        }]
                }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")

#####

def variant_delivery_date_fr(data): 
    
    data= data[["variant_gid","meta_v_delivery_date_fr_meta_gid","meta_v_delivery_date_fr_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)

    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_delivery_date_fr_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "delivery_date_fr",
                                "value":   data.loc[item]["meta_v_delivery_date_fr_value_neu"],
                                "valueType": "STRING"
                        }]
                }}
            ) + "\n")
            
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")
        
def variant_delivery_date_en(data): 
    
    data= data[["variant_gid","meta_v_delivery_date_en_meta_gid","meta_v_delivery_date_en_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)
 
    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_delivery_date_en_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "delivery_date_en",
                                "value":   data.loc[item]["meta_v_delivery_date_en_value_neu"],
                                "valueType": "STRING"
                        }]
                }}
            ) + "\n")
            
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")
        
def variant_delivery_date_it(data): 
    
    data= data[["variant_gid","meta_v_delivery_date_it_meta_gid","meta_v_delivery_date_it_value_neu"]].copy()
    data=data.where(pd_notnull(data), None)

    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "metafields": [{
                                "id":data.loc[item]["meta_v_delivery_date_it_meta_gid"] ,
                                "namespace": "my_fields",
                                "key": "delivery_date_it",
                                "value":   data.loc[item]["meta_v_delivery_date_it_value_neu"],
                                "valueType": "STRING"
                        }]
                }}
            ) + "\n")
                    
    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")        
        
        
        

####

def variant_sku(data): 
    
    data = data[["variant_gid","variant_id","sku"]].copy()
    data = data[("dq_" + data["variant_id"]) != data["sku"]].copy()
    data = data.where(pd_notnull(data), None)


    with open("f05_bulk/variant.jsonl", 'w') as f:
        for item in data.index:

            temp= f.write(json_dumps(
                {
                "input": {
                         "id": data.loc[item]["variant_gid"],
                        "sku": "dq_" + data.loc[item]["variant_id"]
                }}
            ) + "\n")
    return data   

    if len(data) == 0: # File auch kreieren, wenn keine Daten vorhanden sind
        print("Keine Daten vorhanden")

####


def bulk_complete(time=3): 
    bulk_result = shopify_GraphQL().execute(
        #  Im Metafield gelöscht: definition {   namespace type { valueType    }}
        '''

                    mutation {
                      bulkOperationRunQuery(
                        query: """



                                                {
                                                  products(first: 1) {
                                                    edges {
                                                      node {
                                                        id
                                                        vendor
                                                        metafields(namespace: "my_fields", first: 1) {
                                                          edges {
                                                            node {
                                                              key
                                                              value
                                                              id
                                                              definition {
                                                                namespace
                                                                type {
                                                                  valueType
                                                                }
                                                              }
                                                            }
                                                          }
                                                        }
                                                        options {
                                                          name
                                                          position
                                                          values
                                                        }
                                                        variants(first: 1) {
                                                          edges {
                                                            node {
                                                              id
                                                              barcode
                                                              inventoryQuantity
                                                              sku
                                                              weight
                                                              selectedOptions {
                                                                name
                                                                value
                                                              }
                                                              price
                                                              inventoryPolicy
                                                              metafields(first: 1, namespace: "my_fields") {
                                                                edges {
                                                                  node {
                                                                    value
                                                                    key
                                                                    id
                                                                  }
                                                                }
                                                              }
                                                              position
                                                              inventoryItem {
                                                                inventoryLevels(first: 1) {
                                                                  edges {
                                                                    node {
                                                                      location {
                                                                        name
                                                                        id
                                                                      }
                                                                      available
                                                                    }
                                                                  }
                                                                }
                                                                unitCost {
                                                                  amount
                                                                }
                                                                countryCodeOfOrigin
                                                                id
                                                              }
                                                            }
                                                          }
                                                        }
                                                        handle
                                                        status
                                                        productType
                                                        tags
                                                        title
                                                        totalInventory
                                                        totalVariants
                                                        description
                                                      }
                                                    }
                                                  }
                                                }





                        """
                      ) {
                        bulkOperation {
                          id
                          status
                        }
                        userErrors {
                          field
                          message
                        }
                      }
                    }


        ''')   
    time_sleep(time)
 



   
#########    
    
def bulk_media(time=3):     
        bulk_images = shopify_GraphQL().execute(
            '''

                mutation {
                  bulkOperationRunQuery(
                    query: """

                                            {
                                              products(first: 1) {
                                                edges {
                                                  node {
                                                    images(first: 10) {
                                                      edges {
                                                        node {
                                                          src
                                                        }
                                                      }
                                                    }
                                                    id
                                                  }
                                                }
                                              }
                                            }



                    """
                  ) {
                    bulkOperation {
                      id
                      status
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }


            ''')

        time_sleep(time)   
        
        
        
########

# def bulk_inventory(time=3):  
    
#     bulk_result = shopify_GraphQL().execute(
#         '''

#                     mutation {
#                       bulkOperationRunQuery(
#                         query: """


#                                         query MyQuery {
#                                           inventoryItems(first: 10) {
#                                             edges {
#                                               node {
#                                                 id
#                                                 inventoryLevels(first: 10) {
#                                                   edges {
#                                                     node {
#                                                       available
#                                                       location {
#                                                         id
#                                                         name
#                                                       }
#                                                     }
#                                                   }
#                                                 }
#                                               }
#                                             }
#                                           }
#                                         }

                                            
                                            
                                            

#                         """
#                       ) {
#                         bulkOperation {
#                           id
#                           status
#                         }
#                         userErrors {
#                           field
#                           message
#                         }
#                       }
#                     }

#         ''') 
#     time_sleep(time)
        
        
        



######

def inventory_delta(data): 


    data = data[["locationId","InventoryItem_gid","availableDelta"]].copy()
    data = data.where(pd_notnull(data), None)
    data = data.dropna()


    temp_inv = '''locationId: "''' + data["locationId"].unique() + '''",
        inventoryItemAdjustments: ['''



    for item in data.index:
            temp_inv = temp_inv + '{inventoryItemId: "'   +   data.loc[item]["InventoryItem_gid"]  +  '", availableDelta:'   + str(data.loc[item]["availableDelta"] )    + '''}
            '''

    temp_inv= temp_inv +  ''']'''


    temp_inv = '''
            mutation {
      inventoryBulkAdjustQuantityAtLocation(
    '''    + temp_inv +    '''    ) {

    userErrors {
          field
          message
        }
      }
    }
    '''     
    return temp_inv

#######






def g_chat_bot(txt,chat = 1):
    
    # chat 1 ist normaler bot
    # chat 2 ist hourly bot
    
    response = httplib2_Http().request(
        uri='https://chat.googleapis.com/v1/spaces/AAAAzJaKbNU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=XIoXNVIHCjBP406wV-eyyVjM3yr3Hz34IOj9RLj39RU%3D' if chat == 1 else "https://chat.googleapis.com/v1/spaces/AAAAzJaKbNU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KdyF77j1Bd85B-KglU6Sq3mAVom2sWIHj5oWFZbt-rU%3D" ,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json_dumps({'text' : str(txt) + pd_timestamp.now(tz='Europe/Zurich').strftime('%d%b-%H:%M:%S')}),
    )
        

######



def send_mail(to,subject,content): 

    user = 'andrin@evolyze.ch'
    app_password = os_getenv("gmail_app_password")
    
    with yagmail_SMTP({'andrin@evolyze.ch':'dancingqueens_bot'}, app_password) as yag:
        yag.send(to, subject, content)
        print('Sent email successfully')

#########

def dq_read_sql(sql_code):



            host = "dancingqueens.mysql.eu.pythonanywhere-services.com"  if os_getenv('PYTHONANYWHERE_SITE') is not None else '127.0.0.1'
            user = 'dancingqueens'
            passwd = os_getenv("pyth_any_db_pw")
            db = 'dancingqueens$dq'


            if os_getenv('PYTHONANYWHERE_SITE') is not None:

                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host  + '/' + db
                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)

                        temp = pd_read_sql(sql_code,engine)

                        if temp is not None:
                            return temp      

                        engine.dispose()                
                

            else: 
                
                        sshtunnel_SSH_TIMEOUT = 5
                        sshtunnel_TUNNEL_TIMEOUT = 15   
                        
                        with sshtunnel_SSHTunnelForwarder(

                            ('ssh.eu.pythonanywhere.com'),
                            ssh_password=os_getenv("pyth_any_pw"),
                            ssh_username='dancingqueens',
                            remote_bind_address=('dancingqueens.mysql.eu.pythonanywhere-services.com', 3306)

                            ) as server:

                                        server.start()

                                        port = ':' + str(server.local_bind_port)
                                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host + port  + '/' + db
                                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)

                                        temp = pd_read_sql(sql_code,engine)

                                        if temp is not None:
                                            return temp                            

                                        engine.dispose()
                                        server.stop()   

                    
###################


def dq_to_sql(data, data_name, mode = "append",index_table=False):
            

        
        dtypedict = {}


        for i,j in zip(data.columns, data.dtypes):
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy_types_VARCHAR(length=255)})

            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy_types_DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy_types_Float(precision=3, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy_types_INTEGER()})

            if "bool" in str(j):
                dtypedict.update({i: sqlalchemy_types_Boolean()})      
        
        
        
        
            # bezüglich data_type / dtype= https://stackoverflow.com/questions/53294611/pandas-to-sql-changing-datatype-in-database-table
        
            host = "dancingqueens.mysql.eu.pythonanywhere-services.com"  if os_getenv('PYTHONANYWHERE_SITE') is not None else '127.0.0.1'
            user = 'dancingqueens'
            passwd = os_getenv("pyth_any_db_pw")
            db = 'dancingqueens$dq'

            if os_getenv('PYTHONANYWHERE_SITE') is not None:
                        
                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host  + '/' + db
                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)

                        data.to_sql(name = data_name,con = engine,if_exists=mode, index=index_table, dtype=dtypedict)

                        engine.dispose()      
                        return

                        
                        
            else: 
                
                        sshtunnel_SSH_TIMEOUT = 5
                        sshtunnel_TUNNEL_TIMEOUT = 15   
                        
                        with sshtunnel_SSHTunnelForwarder(

                            ('ssh.eu.pythonanywhere.com'),
                            ssh_password=os_getenv("pyth_any_pw"),
                            ssh_username='dancingqueens',
                            remote_bind_address=('dancingqueens.mysql.eu.pythonanywhere-services.com', 3306)

                            ) as server:

                                        server.start()

                                        port = ':' + str(server.local_bind_port)
                                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host + port  + '/' + db
                                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)
                                        
                                        

                                        data.to_sql(name = data_name,con = engine,if_exists=mode, index=index_table, dtype=dtypedict)

                                        engine.dispose()                                        
                                        server.stop()
                                        return
                                        
    
                                        
                                        
  



                                          
                                        
#########

def dq_execute_sql(sql_code):



            host = "dancingqueens.mysql.eu.pythonanywhere-services.com"  if os_getenv('PYTHONANYWHERE_SITE') is not None else '127.0.0.1'
            user = 'dancingqueens'
            passwd = os_getenv("pyth_any_db_pw")
            db = 'dancingqueens$dq'


            if os_getenv('PYTHONANYWHERE_SITE') is not None:

                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host  + '/' + db
                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)

                        temp = engine.execute(sql_code)  
                        return
                        engine.dispose()                
                

            else: 
                
                        sshtunnel_SSH_TIMEOUT = 5
                        sshtunnel_TUNNEL_TIMEOUT = 15   
                        
                        with sshtunnel_SSHTunnelForwarder(

                            ('ssh.eu.pythonanywhere.com'),
                            ssh_password=os_getenv("pyth_any_pw"),
                            ssh_username='dancingqueens',
                            remote_bind_address=('dancingqueens.mysql.eu.pythonanywhere-services.com', 3306)

                            ) as server:

                                        server.start()

                                        port = ':' + str(server.local_bind_port)
                                        conn_addr = 'mysql://' + user + ':' + passwd + '@' + host + port  + '/' + db
                                        engine = sqlalchemy_create_engine(conn_addr, pool_recycle=280)

                                        temp = engine.execute(sql_code)                      
                                        return
                                        engine.dispose()
                                        server.stop()                                           
                                        
                                        


    