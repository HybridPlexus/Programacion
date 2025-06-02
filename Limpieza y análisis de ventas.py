import pandas as pd


def process_sales_data(file_path):
    try:
        df = pd.read_csv("C:\\Users\\hp\\PycharmProjects\\PythonProject1\\Dataset\\sales_data.csv")
        print("Archivo CSV cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el archivo CSV: {e}")
        return None, None, None

    df_clean = df.dropna(subset=['sales']).copy()

    df_clean.loc[:, 'region'] = df_clean['region'].fillna('Desconocido')
    df_clean.loc[:, 'ventas_con_iva'] = df_clean['sales'] * 1.16

    sales_by_region = df_clean.groupby('region')['sales'].sum().reset_index()
    avg_sales_by_product = df_clean.groupby('product')['sales'].mean().round(2).reset_index()

    return df_clean, sales_by_region, avg_sales_by_product

if __name__ == "__main__":
    file_path = "sales_data.csv"
    processed_df, region_sales, product_avg = process_sales_data(file_path)

    print("\nDataFrame procesado:")
    print(processed_df)

    print("\nTotal de ventas por región:")
    print(region_sales)

    print("\nPromedio de ventas por producto:")
    print(product_avg)