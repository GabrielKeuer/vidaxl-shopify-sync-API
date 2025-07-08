import os
import requests
import pandas as pd
from time import sleep

# Credentials fra Github secrets
VIDAXL_EMAIL = os.environ['VIDAXL_EMAIL']
VIDAXL_API_KEY = os.environ['VIDAXL_API_KEY']
SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_SHOP = os.environ['SHOPIFY_SHOP']

BASE_URL = 'https://b2b.vidaxl.com/api_customer/products'
BATCH_SIZE = 500

def hent_vidaxl():
    all_products = []
    offset = 0
    while True:
        params = {'limit': BATCH_SIZE, 'offset': offset}
        response = requests.get(
            BASE_URL,
            params=params,
            auth=(VIDAXL_EMAIL, VIDAXL_API_KEY)
        )
        if response.status_code != 200:
            print(f"Fejl: {response.status_code} - {response.text}")
            break
        response_json = response.json()
        products = response_json.get("data", [])
        if not products:
            break
        all_products.extend(products)
        print(f"Hentet {len(all_products)} produkter ...")
        if len(products) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
        sleep(1)
    df = pd.DataFrame(all_products)
    return df[['code', 'price', 'quantity', 'updated_at']].rename(columns={
        'code': 'SKU',
        'price': 'B2B price',
        'quantity': 'Stock',
        'updated_at': 'Sidst ændret'
    })

def hent_shopify_variants():
    url = f"https://{SHOPIFY_SHOP}.myshopify.com/admin/api/2023-07/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json"
    }
    # Query for alle varianter (kun SKU, ID, pris, cost, lager)
    # For demo – for rigtigt bulk bør du bruge Bulk API!
    query = '''
    {
      productVariants(first: 250) {
        edges {
          node {
            id
            sku
            price
            inventoryQuantity
            inventoryItem {
              cost
            }
          }
        }
      }
    }
    '''
    r = requests.post(url, headers=headers, json={'query': query})
    edges = r.json()['data']['productVariants']['edges']
    variants = []
    for edge in edges:
        node = edge['node']
        variants.append({
            'variant_id': node['id'],
            'SKU': node['sku'],
            'price': float(node['price']),
            'cost': float(node['inventoryItem']['cost']) if node['inventoryItem']['cost'] else 0.0,
            'inventoryQuantity': node['inventoryQuantity']
        })
    return pd.DataFrame(variants)

def beregn_salgspris(cost):
    # Juster til din egen prisregel
    return round(cost * 1.6)

def delta_detection(df_vidaxl, df_shopify):
    df = pd.merge(df_vidaxl, df_shopify, on='SKU', how='inner', suffixes=('_vidaxl', '_shopify'))
    to_update = []
    for _, row in df.iterrows():
        # Find forskelle
        lager_ændret = row['Stock_vidaxl'] != row['inventoryQuantity']
        pris_ændret = row['B2B price_vidaxl'] != row['cost']
        mutation = {}
        if lager_ændret and not pris_ændret:
            mutation = {
                "id": row['variant_id'],
                "inventoryQuantity": row['Stock_vidaxl']
            }
        elif pris_ændret:
            salgspris = beregn_salgspris(row['B2B price_vidaxl'])
            mutation = {
                "id": row['variant_id'],
                "price": str(salgspris),
                "cost": str(row['B2B price_vidaxl']),
                "inventoryQuantity": row['Stock_vidaxl'] if lager_ændret else None
            }
        if mutation:
            to_update.append(mutation)
    return to_update

def main():
    print("Henter vidaXL...")
    df_vidaxl = hent_vidaxl()
    print("Henter Shopify varianter...")
    df_shopify = hent_shopify_variants()
    print("Sammenligner og danner mutationsliste...")
    mutations = delta_detection(df_vidaxl, df_shopify)
    # Her skal du bygge mutationer til Shopify Bulk API (jsonl eller graphql) og uploade.
    print(f"Antal varianter der skal opdateres: {len(mutations)}")
    # For demo, gem til fil:
    pd.DataFrame(mutations).to_csv("shopify_mutations.csv", index=False, encoding='utf-8-sig')
    print("Klar til upload til Shopify Bulk API!")

if __name__ == "__main__":
    main()
