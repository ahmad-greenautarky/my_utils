import psycopg2

# Cloud database credentials
conn = psycopg2.connect(
        host="80.158.21.182",
        database="ga_database",
        user="root",
        password="b6gxcpQu5yEuntf",
        port=5432
)

cur = conn.cursor()

# Retrieve the image by record ID
record_id = 13  # Change to the ID you want
cur.execute("SELECT signature_image FROM fact_installation_records WHERE id = %s", (record_id,))
result = cur.fetchone()

if result:
    image_data = result[0]
    
    # Save to file
    with open('retrieved_signature.png', 'wb') as f:
        f.write(image_data)
    
    print("Image retrieved and saved as 'retrieved_signature.png'")
else:
    print("No record found")

cur.close()
conn.close()