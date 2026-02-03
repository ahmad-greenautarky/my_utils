import psycopg2
from datetime import date

# Cloud database credentials
conn = psycopg2.connect(
        host="80.158.xx.182",
        database="ga_database",
        user="root",
        password="xx",
        port=5432
)

cur = conn.cursor()

# Your data
user_id = 123
name = "Ahmad Doe"
ki_butler = 1
radiator_thermostat = 5
temp_humidity_screen = 5
acknowledged = True
gdpr_acknowledged = True
signature_image_data = b"..."  # binary image data

# Save signature image to filesystem
signature_filename = r"C:\Users\ahmad\Desktop\test_signature.png"
# Read PNG image as binary
with open(signature_filename, 'rb') as f:
    signature_image_data = f.read()

cur.execute("""
    INSERT INTO fact_installation_records 
    (user_name, ki_butler_count, radiator_thermostat_count, 
     temp_humidity_screen_count, acknowledged, gdpr_acknowledged, 
     installation_date, signature_image)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""", (name, ki_butler, radiator_thermostat, temp_humidity_screen, 
      acknowledged, gdpr_acknowledged, date.today(), signature_image_data))

# Commit the transaction
conn.commit()

# Close connection
cur.close()
conn.close()

print("Installation record with signature saved!")