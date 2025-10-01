import pandas as pd
import os
ruta_data = os.path.join(os.path.expanduser("~"), "Downloads") + '\\'

#clientes
df_customer = pd.read_csv(ruta_data + 'olist_customers_dataset.csv', delimiter=",")
df_customer['customer_id'] = df_customer['customer_id'].astype(str)
df_customer = df_customer[['customer_id', 'customer_city', 'customer_state']]

#solo ordenes
df_order = pd.read_csv(ruta_data + 'olist_orders_dataset.csv', delimiter=",")
df_order['order_id']=df_order['order_id'].astype(str)
df_order['customer_id']=df_order['customer_id'].astype(str)
df_order['order_delivered_carrier_date'] = pd.to_datetime(df_order['order_delivered_carrier_date'],format='%Y-%m-%d %H:%M:%S').dt.normalize()
df_order['order_purchase_timestamp'] = pd.to_datetime(df_order['order_purchase_timestamp'],format='%Y-%m-%d %H:%M:%S').dt.normalize()
df_order['order_delivered_customer_date'] = pd.to_datetime(df_order['order_delivered_customer_date'],format='%Y-%m-%d %H:%M:%S').dt.normalize()
df_order['order_estimated_delivery_date'] = pd.to_datetime(df_order['order_estimated_delivery_date'],format='%Y-%m-%d %H:%M:%S').dt.normalize()
df_order = df_order.merge(df_customer, left_on='customer_id', right_on='customer_id', how='left')

#sellers
df_sellers = pd.read_csv(ruta_data + 'olist_sellers_dataset.csv', delimiter=",")
df_sellers['seller_id'] = df_sellers['seller_id'].astype(str)

#payments
df_payments = pd.read_csv(ruta_data + 'olist_order_payments_dataset.csv', delimiter=",")
df_payments['order_id']=df_payments['order_id'].astype(str)
df_payments = df_payments.drop_duplicates(subset='order_id')
df_payments = df_payments[['order_id', 'payment_type', 'payment_value', 'payment_installments']]

#items
df_items = pd.read_csv(ruta_data + 'olist_order_items_dataset.csv', delimiter=",")
df_items['seller_id'] = df_items['seller_id'].astype(str)
df_items['product_id'] = df_items['product_id'].astype(str)
df_items['shipping_limit_date'] = pd.to_datetime(df_items['shipping_limit_date'],format='%Y-%m-%d %H:%M:%S').dt.normalize()
df_items['order_total']=df_items['price'].astype(float) + df_items['freight_value'].astype(float)
df_items_sellers = df_items.merge(df_sellers, left_on='seller_id', right_on='seller_id', how='left')

#productos
df_products = pd.read_csv(ruta_data + 'olist_products_dataset.csv', delimiter=",")
df_products['product_id']=df_products['product_id'].astype(str)
df_products = df_products[['product_id', 'product_category_name']]

#cruces - mi matriz principal items
df_items_sellers = df_items_sellers.merge(df_order, left_on='order_id', right_on='order_id', how='left')
df_items_sellers = df_items_sellers.merge(df_payments, left_on='order_id', right_on='order_id', how='left')
df_items_sellers = df_items_sellers.merge(df_products, left_on='product_id', right_on='product_id', how='left')

#df_items_sellers = df_items_sellers.drop_duplicates(subset='order_id')

#condicionales
def cumplimiento_entrega(row):
    if row['order_delivered_customer_date'] < row['order_estimated_delivery_date']:
        return 'Se realizó entrega antes de fecha estimada'
    elif row['order_delivered_customer_date'] == row['order_estimated_delivery_date']:
        return 'Se realizó entrega el día de fecha estimada'
    elif row['order_delivered_customer_date'] > row['order_estimated_delivery_date']:
        return 'Se realizó entrega despues de la fecha estimada'
    elif pd.isna(row['order_delivered_customer_date']):
        if row['order_status'] == 'approved':
          return  'Pendiente de entrega'
        elif row['order_status'] == 'canceled':
           return 'Orden cancelada'
        elif row['order_status'] == 'invoiced':
            return 'Pendiente de entrega'
        elif row['order_status'] == 'processing':
            return 'Pendiente de entrega'
        elif row['order_status'] == 'unavailable':
            return 'Orden no disponible, en proceso de cancelación'
        elif row['order_status'] == 'shipped':
            return 'Pendiente de entrega'
        return 'Pendiente de entrega'
    else:
        'Pendiente de entrega'
df_items_sellers['cumplimiento_entrega'] = df_items_sellers.apply(cumplimiento_entrega, axis=1)


#saber si se despacho o no a tiempo
def cumplimiento_despacho(row):
    if row['order_delivered_carrier_date'] == row['shipping_limit_date']:
        return 'Seller despacha el mismo día límite de despacho'
    elif row['order_delivered_carrier_date'] < row['shipping_limit_date']:
        return 'Seller despacha antes de límite de despacho'
    elif row['order_delivered_carrier_date'] > row['shipping_limit_date']:
        return 'Seller despacha despues de límite de despacho'
    elif pd.isna(row['order_delivered_carrier_date']):
        if row['order_status'] == 'approved':
          return  'Pendiente de despacho por seller'
        elif row['order_status'] == 'canceled':
           return 'Seller no despachó y se canceló'
        elif row['order_status'] == 'invoiced':
            return 'Pendiente de despacho por seller'
        elif row['order_status'] == 'processing':
            return 'Pendiente de despacho por seller'
        elif row['order_status'] == 'processing':
            return 'Pendiente de despacho por seller'
        elif row['order_status'] == 'unavailable':
            return 'No está disponible y no se atenderá por seller'
        elif row['order_status'] == 'shipped':
            return 'Despachado por seller pero sin fecha límite'
        else:
            return 'Pendiente de revisión'
    else:
        'Pendiente de despacho por seller'
        
df_items_sellers['cumplimiento_despacho'] = df_items_sellers.apply(cumplimiento_despacho, axis=1)
             
    
    

print(df_items_sellers)

df_items_sellers.to_excel(ruta_data + 'ORDER.ECOM.xlsx', index=False)