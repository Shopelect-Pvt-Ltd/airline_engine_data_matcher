import json
from utils.log import get_logger
logger = get_logger()
import psycopg2
import pika
import time
import os
from uuid import uuid4
from datetime import datetime

logger = logging.getLogger(__name__)



RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RabbitMQ_PORT = os.getenv('RABBITMQ_PORT')
PG_HOST = os.getenv('PG_HOST')
PG_AIRLINES_DB = os.getenv('PG_AIRLINES_DB')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')


AIRLINE_ENGINE_PARSER_OUTPUT_Q_RABBITMQ = "airline_engine_scraper_status_q"


# --- Database connection ---
def get_db_connection():
    return psycopg2.connect(
        # host=PG_HOST,
        host='10.200.20.73',
        database=PG_AIRLINES_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

from datetime import datetime

def to_float(v):
            try:
                return float(v) if v not in (None, "", " ") else 0.0
            except Exception:
                return 0.0

def normalize_date(date_value):
    """
    Converts 'DD-MM-YYYY' or 'DD/MM/YYYY' or already valid ISO date into 'YYYY-MM-DD'.
    Returns None if invalid or empty.
    """
    if not date_value:
        return None

    if isinstance(date_value, datetime):
        return date_value.date()  # already a datetime

    if isinstance(date_value, str):
        date_value = date_value.strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_value, fmt).date()
            except ValueError:
                continue
    return None

def normalize_numeric(value):
    """Convert empty strings or invalid numerics to None or float"""
    if value in (None, "", " ", "NA", "N/A", "-", "--"):
        return None
    try:
        return float(value)
    except Exception:
        return None

# --- Create RabbitMQ channel ---
def create_rabbitmq_channel(queue_name):
    credentials = pika.PlainCredentials('finkraft', 'finkai@123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            # host=RABBITMQ_HOST,
            host='10.200.20.83',
            port=RabbitMQ_PORT,
            virtual_host='/',
            credentials=credentials
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    return channel


def select_and_update_matcher(target_table, matched_guid, record, invoice_guid, ticket_number, pnr, connection, cursor):
    """
    Fetch detailed data from airline_engine_booking or indigo_scraper_priority,
    combine with parsed invoice data, and insert into airline_engine_data_matcher.
    """
    import uuid
    from datetime import datetime
    invoice_guid = invoice_guid
    try:
        logger.info(f"[MATCHER START] Target Table: {target_table}, GUID: {matched_guid}")

        # -----------------------------
        # 1️⃣ Fetch data from target table
        # -----------------------------
        if target_table == "airline_engine_booking":
            cursor.execute("""
                SELECT 
                    airline_name,
                    za_data->>'Type' AS type,
                    za_data->>'Financial Year' AS financial_year,
                    (za_data->>'Transaction_Date') AS transaction_date,
                    za_data->>'Workspace' AS workspace,
                    za_data->>'Ticket_Number' AS ticket_number,
                    za_data->>'PNR' AS pnr,
                    za_data->>'Ticket/PNR' AS ticket_pnr,
                    (za_data->>'Transaction_Amount') AS transaction_amount,
                    za_data->>'Traveller Name' AS traveller_name,
                    za_data->>'Class' AS class,
                    za_data->>'Customer_GSTIN' AS customer_gstin,
                    za_data->>'Agency_Invoice_Number' AS agency_invoice_number,
                    za_data->>'Location' AS location,
                    za_data->>'Domestic/International' AS domestic_international,
                    (za_data->>'GST_Rate') AS gst_rate,
                    za_data->>'Origin' AS origin,
                    (za_data->>'Booking_GST') AS booking_gst,
                    (za_data->>'K3') AS k3,
                    za_data->>'Invoice - Name as per GST portal' AS invoice_name_as_per_gst_portal,
                    za_data->>'Supplier_GSTIN' AS supplier_gstin
                FROM airline_engine_booking
                WHERE za_data->>'Ticket/PNR' = %s OR za_data->>'Ticket/PNR' = %s
            """, (ticket_number, pnr))
            booking_row = cursor.fetchone()

        elif target_table == "indigo_scraper_priority":
            cursor.execute("""
                SELECT 
                    'indigo' AS airline_name,
                    "FY" AS financial_year,
                    "Transaction_Date" AS transaction_date,
                    "Workspace" AS workspace,
                    "Ticket/PNR" AS ticket_number,
                    "Ticket/PNR" AS pnr,
                    "Ticket/PNR" AS ticket_pnr,
                    NULL AS transaction_amount,
                    traveller_name,
                    NULL AS class,
                    "Customer_GSTIN" AS customer_gstin,
                    NULL AS agency_invoice_number,
                    Origin AS location,
                    NULL AS domestic_international,
                    NULL AS gst_rate,
                    Origin AS origin,
                    NULL AS booking_gst,
                    NULL AS k3,
                    NULL AS invoice_name_as_per_gst_portal,
                    NULL AS supplier_gstin
                FROM indigo_scraper_priority
                WHERE "Ticket/PNR" = %s OR "Ticket/PNR" = %s
            """, (ticket_number, pnr))
            booking_row = cursor.fetchone()
        else:
            logger.warning(f"Unknown target_table: {target_table}")
            return

        if not booking_row:
            logger.warning(f"No record found in {target_table} for {ticket_number}/{pnr}")
            return

        booking_columns = [desc[0] for desc in cursor.description]
        booking_data = dict(zip(booking_columns, booking_row))

        # -----------------------------
        # 2️⃣ Get parsed data (from AI extraction)
        # -----------------------------
        # Extract values safely
        igst = to_float(record.get("igst_amount"))
        cgst = to_float(record.get("cgst_amount"))
        sgst = to_float(record.get("sgst_amount"))
        igst_rate = to_float(record.get("igst_rate"))
        cgst_rate = to_float(record.get("cgst_rate"))
        sgst_rate = to_float(record.get("sgst_rate"))

        parsed_data = {
            "invoice_number": record.get("airline_invoice_number") or record.get("credit_debit_number"),
            "invoice_date": record.get("airline_invoice_date") or record.get("credit_debit_date"),
            "original_invoice_number": record.get("original_invoice_number"),
            "tax_rate": igst_rate + cgst_rate + sgst_rate,
            "taxable": record.get("taxable_value"),
            "cgst": record.get("cgst_amount"),
            "sgst": record.get("sgst_amount"),
            "igst": record.get("igst_amount"),
            "gst_amount": igst + cgst + sgst,
            "total_amount": record.get("total_amount"),
            "supplier_gstin": record.get("customer_gst_number"),
        }

        # -----------------------------
        # 3️⃣ Combine all fields for insertion
        # -----------------------------
        insert_data = {
            "id": str(uuid.uuid4()),
            **booking_data,   # from either booking or indigo table
            **parsed_data,     # parsed values from invoice
            "invoice_guid" : invoice_guid
        }
        insert_data["transaction_date"] = normalize_date(insert_data.get("transaction_date"))
        insert_data["invoice_date"] = normalize_date(insert_data.get("invoice_date"))
        # normalize numeric values
        for num_field in ["gst_rate", "tax_rate", "taxable", "cgst", "sgst", "igst", "gst_amount", "total_amount", "booking_gst", "k3"]:
            insert_data[num_field] = normalize_numeric(insert_data.get(num_field))


        insert_query = """
            INSERT INTO airline_engine_data_matcher (
                id,
                airline_name, type, financial_year, transaction_date, workspace,
                ticket_number, pnr, ticket_pnr, transaction_amount, traveller_name,
                class, customer_gstin, agency_invoice_number, location, domestic_international,
                gst_rate, origin, booking_gst, k3, invoice_name_as_per_gst_portal, supplier_gstin,
                invoice_number, invoice_date, original_invoice_number, tax_rate,
                taxable, cgst, sgst, igst, gst_amount, total_amount,invoice_guid
            )
            VALUES (
                %(id)s,
                %(airline_name)s, %(type)s, %(financial_year)s, %(transaction_date)s, %(workspace)s,
                %(ticket_number)s, %(pnr)s, %(ticket_pnr)s, %(transaction_amount)s, %(traveller_name)s,
                %(class)s, %(customer_gstin)s, %(agency_invoice_number)s, %(location)s, %(domestic_international)s,
                %(gst_rate)s, %(origin)s, %(booking_gst)s, %(k3)s, %(invoice_name_as_per_gst_portal)s, %(supplier_gstin)s,
                %(invoice_number)s, %(invoice_date)s, %(original_invoice_number)s, %(tax_rate)s,
                %(taxable)s, %(cgst)s, %(sgst)s, %(igst)s, %(gst_amount)s, %(total_amount)s , %(invoice_guid)s
            )
            ON CONFLICT (invoice_guid)
            DO UPDATE SET
                airline_name = EXCLUDED.airline_name,
                type = EXCLUDED.type,
                financial_year = EXCLUDED.financial_year,
                transaction_date = EXCLUDED.transaction_date,
                workspace = EXCLUDED.workspace,
                ticket_number = EXCLUDED.ticket_number,
                pnr = EXCLUDED.pnr,
                ticket_pnr = EXCLUDED.ticket_pnr,
                transaction_amount = EXCLUDED.transaction_amount,
                traveller_name = EXCLUDED.traveller_name,
                class = EXCLUDED.class,
                customer_gstin = EXCLUDED.customer_gstin,
                agency_invoice_number = EXCLUDED.agency_invoice_number,
                location = EXCLUDED.location,
                domestic_international = EXCLUDED.domestic_international,
                gst_rate = EXCLUDED.gst_rate,
                origin = EXCLUDED.origin,
                booking_gst = EXCLUDED.booking_gst,
                k3 = EXCLUDED.k3,
                invoice_name_as_per_gst_portal = EXCLUDED.invoice_name_as_per_gst_portal,
                supplier_gstin = EXCLUDED.supplier_gstin,
                invoice_number = EXCLUDED.invoice_number,
                invoice_date = EXCLUDED.invoice_date,
                original_invoice_number = EXCLUDED.original_invoice_number,
                tax_rate = EXCLUDED.tax_rate,
                taxable = EXCLUDED.taxable,
                cgst = EXCLUDED.cgst,
                sgst = EXCLUDED.sgst,
                igst = EXCLUDED.igst,
                gst_amount = EXCLUDED.gst_amount,
                total_amount = EXCLUDED.total_amount,
                invoice_guid = EXCLUDED.invoice_guid;
        """

        cursor.execute(insert_query, insert_data)
        connection.commit()
        logger.info(f"[MATCHER SUCCESS] Inserted/Updated record in airline_engine_data_matcher")

    except Exception as e:
        connection.rollback()
        logger.exception(f"[MATCHER ERROR] Failed for {invoice_guid}: {e}")



# --- Core processing for each message ---
def process_each_message(blob_info):
    connection = None
    try:
        parsed_guid = blob_info.get("guid")
        data = blob_info.get("data", {})
        file_hash = data.get("file_hash")

        parsed_data1 = data.get("tickets_data", [])
        parsed_data2 = data.get("amounts_data", [])
        parsed_data_single = data.get("parsed_data", [])

        connection = get_db_connection()
        cursor = connection.cursor()

        logger.info(f"🔹 Processing GUID: {parsed_guid}")

        # Define helper for processing records
        def process_record(record):
            logger.info(f"[PROCESS START] ")

            try:
                airline = record.get("airline_name")
                doc_type = record.get("document_type")
                pnr = None

                # --- Extract PNR safely ---
                if isinstance(record.get("ticket_pnr"), dict):
                    pnr_list = record["ticket_pnr"].get("pnr") or []
                    pnr = pnr_list[0] if pnr_list else None
                else:
                    pnr = record.get("pnr")

                # --- Extract PNR safely ---
                if isinstance(record.get("ticket_pnr"), dict):
                    ticket_list = record["ticket_pnr"].get("ticket_number") or []
                    ticket_number = ticket_list[0] if ticket_list else None
                else:
                    ticket_number = record.get("ticket_number")
                    ticket_number = ticket_number[-10:]


                logger.info(f"Processing parsed record for airline: {airline}, doc_type: {doc_type}, pnr: {pnr}, ticket_number: {ticket_number}")
                # --- Get invoice info ---

                cursor.execute("""
                    SELECT guid, source, booking_guid, indigo_guid , airline_name , updated_at , file_hash 
                    FROM airline_engine_invoice
                    WHERE guid=%s 
                """, (parsed_guid,))
                invoice = cursor.fetchone()

                if not invoice:
                    logger.warning(f"No invoice found for GUID: {parsed_guid} / hash: {file_hash}")
                    return

                invoice_guid, invoice_source, booking_guid, indigo_guid , t_airline_name , f_parsed_at,f_file_hash = invoice
                logger.info(f"Found airline_engine_invoice : {invoice_guid}, source: {invoice_source}, booking_guid: {booking_guid}, indigo_guid: {indigo_guid} , airline_name: {t_airline_name}")
                match = None
                target_table = None

                # If booking not found, try indigo_scraper_priority
                if indigo_guid :
                    logger.info(f"searching in indigo_scraper_priority")
                    cursor.execute("""
                        SELECT guid, transaction_type
                        FROM indigo_scraper_priority
                        WHERE guid=%s 
                    """, (indigo_guid))
                    match = cursor.fetchone()
                    if match:
                        target_table = "indigo_scraper_priority"
                        if not match:
                            logger.info(f"No matching booking/indigo found for PNR: {pnr}")

                elif not indigo_guid and t_airline_name == 'indigo' and ticket_number or pnr : 
                    logger.info(f"searching in indigo_scraper_priority with pnr")
                    cursor.execute("""
                                        SELECT guid, "Transaction_Type"
                                        FROM indigo_scraper_priority
                                        WHERE "Ticket/PNR" = %s OR "Ticket/PNR" = %s
                                    """, (ticket_number, pnr))

                    match = cursor.fetchone()
                    if match:
                        target_table = "indigo_scraper_priority"
                        if not match:
                            logger.info(f"No matching booking/indigo found for PNR: {pnr}")

                # Try booking first
                if not match and booking_guid :
                    logger.info(f"searching in airline_engine_booking")
                    cursor.execute("""
                        SELECT guid, za_data->>'Transaction_Type'
                        FROM airline_engine_booking
                        WHERE guid=%s
                    """, (booking_guid,))
                    match = cursor.fetchone()
                    if match:
                        target_table = "airline_engine_booking"

                elif not match and not booking_guid  and t_airline_name != 'indigo' and  ticket_number or pnr : 
                    logger.info(f"searching in airline_engine_booking")
                    cursor.execute("""
                        SELECT guid, za_data->>'Transaction_Type'
                        FROM airline_engine_booking
                        WHERE za_data->>'Ticket/PNR' = %s or  za_data->>'Ticket/PNR' = %s 
                    """, (ticket_number, pnr))
                    match = cursor.fetchone()
                    if match:
                        target_table = "airline_engine_booking"


                if match : 
                    logger.info(f"match found : {match}")
                    matched_guid, transaction_type = match
                    select_and_update_matcher(target_table, matched_guid, record, invoice_guid, ticket_number, pnr, connection, cursor)
                else : 
                    target_table = 'airline_za_scraper_parser_matching_table'
                    matched_guid = None
                    transaction_type = None
                    # update_airline_za_parser_matching_table()
                    # --- Update correct table ---
                    current_datetime = datetime.now()
                    current_date_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    guid = None
                    airline_name = t_airline_name
                    invoice_guid = invoice_guid
                    invoice_status = 'invoice_not_matched'
                    parsed_at = f_parsed_at
                    matched_at = current_date_str
                    invoice_filehash = f_file_hash
                    try : 
                        # insert final table of Airline_za_parser_matching_table
                        insert_query = """
                        INSERT INTO airline_za_scraper_parser_matching_table (
                                guid,
                                airline_name,
                                invoice_guid,
                                invoice_status,
                                parsed_at,
                                matched_at,
                                file_hash
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (invoice_guid)
                            DO UPDATE SET
                                airline_name = EXCLUDED.airline_name,
                                invoice_status = EXCLUDED.invoice_status,
                                parsed_at = EXCLUDED.parsed_at,
                                matched_at = EXCLUDED.matched_at,
                                file_hash = EXCLUDED.file_hash;
                        """
                        cursor.execute(insert_query, (matched_guid,airline_name, invoice_guid, invoice_status,parsed_at,matched_at,invoice_filehash))
                        connection.commit()
                        logger.info(f" Inserted or updated invoice_guid {invoice_guid} successfully")

                        logger.info(f"----> Inserted → airline_za_scraper_parser_matching_table ")
                        return 

                    except Exception as e:
                        connection.rollback()  # rollback only this transaction
                        logger.exception(f" Error inserting  {target_table}: {e}")

                # --- Determine invoice status ---
                invoice_status = None
                if transaction_type == "Invoice" and doc_type in ['INV','DB','BOS','BOS-DB']:
                    invoice_status = "invoice_received"
                elif transaction_type == "Refund" and doc_type in ['CR','BOS-CR','CN']:
                    invoice_status = "refund_received"
                else:
                    logger.info(f"No matching status for {transaction_type}/{doc_type}")

                # --- Update correct table ---
                # logger.info(f"--- Updating in table: {target_table} ---")
                # logger.info(f"invoice_status     : {invoice_status}")
                # logger.info(f"invoice_source     : {invoice_source}")
                # logger.info(f"invoice_guid       : {invoice_guid}")
                # logger.info(f"invoice_file_hash  : {file_hash}")
                # logger.info(f"matched_guid       : {matched_guid}")
                update_query = f"""
                    UPDATE {target_table}
                    SET invoice_status=%s,
                        invoice_source=%s,
                        invoice_guid=%s,
                        invoice_file_hash=%s
                    WHERE guid=%s
                """
                cursor.execute(update_query, (invoice_status, invoice_source, invoice_guid, file_hash, matched_guid))
                connection.commit() 
                logger.info(f"----> Updated {target_table} ")

            except Exception as e:
                connection.rollback()  # rollback only this transaction
                logger.exception(f"----> Error updating {target_table}: {e}")
            
            current_datetime = datetime.now()
            current_date_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            guid = matched_guid
            airline_name = t_airline_name
            invoice_guid = invoice_guid
            invoice_status = invoice_status
            parsed_at = f_parsed_at
            matched_at = current_date_str
            invoice_filehash = f_file_hash
            
            try : 
                # insert final table of Airline_za_parser_matching_table
                insert_query = """
                INSERT INTO airline_za_scraper_parser_matching_table (
                        guid,
                        airline_name,
                        invoice_guid,
                        invoice_status,
                        parsed_at,
                        matched_at,
                        file_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (invoice_guid)
                    DO UPDATE SET
                        airline_name = EXCLUDED.airline_name,
                        invoice_status = EXCLUDED.invoice_status,
                        parsed_at = EXCLUDED.parsed_at,
                        matched_at = EXCLUDED.matched_at,
                        file_hash = EXCLUDED.file_hash;
                """
                cursor.execute(insert_query, (matched_guid,airline_name, invoice_guid, invoice_status,parsed_at,matched_at,invoice_filehash))
                connection.commit()
                logger.info(f" Inserted or updated invoice_guid {invoice_guid} successfully")

                logger.info(f"----> Inserted → airline_za_scraper_parser_matching_table ")
                return 

            except Exception as e:
                connection.rollback()  # rollback only this transaction
                logger.exception(f" Error inserting  {target_table}: {e}")


            logger.info(f"[PROCESS COMPLETE] GUID={parsed_guid} | Airline={t_airline_name} | Status={invoice_status} | FileHash={file_hash}")


        # --- Process parsed data types ---
        if parsed_data1 and parsed_data2 and len(parsed_data1) != 0 and len(parsed_data2) != 0:
            logger.info("Processing tickets_data and amounts_data...")
            for ticket in parsed_data1:
                process_record(ticket)

        elif parsed_data_single and isinstance(parsed_data_single, list):
            logger.info("Processing single parsed_data list...")
            for ticket in parsed_data_single:
                process_record(ticket)

        else:
            logger.warning("No valid parsed data found in message.")

        connection.commit()

    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Error processing GUID {blob_info.get('guid')}: {e}")

    finally:
        if connection:
            connection.close()


# --- RabbitMQ message callback ---
def callback(ch, method, properties, body):
    global local_messages
    local_messages.append(body)
    logger.info(f'Received message: {body}')

    if local_messages:
        while local_messages:
            msg = local_messages.pop(0)
            try:
                blob_info = json.loads(msg)
                process_each_message(blob_info)
            except json.JSONDecodeError:
                logger.error("Invalid JSON format in message.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("✅ Acknowledged processed messages.")
        local_messages = []


# --- Main Listener Loop ---
local_messages = []

while True:
    logger.info("Fetching Messages!! ---- " + datetime.now().strftime('%d-%m-%y %H:%M:%S'))

    # Connect to RabbitMQ
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            channel = create_rabbitmq_channel(AIRLINE_ENGINE_PARSER_OUTPUT_Q_RABBITMQ)
            logger.info("✅ Successfully connected to RabbitMQ!")
            break
        except Exception as e:
            logger.exception("Failed to connect to RabbitMQ. Retrying in 10 sec...")
            time.sleep(10)

    # Consume messages
    while True:
        try:
            logger.info("PARSER : Fetching Messages!! ---- " + datetime.now().strftime('%d-%m-%y %H:%M:%S'))
            channel.basic_qos(prefetch_count=10)

            if len(local_messages) == 0:
                channel.basic_consume(
                    queue=AIRLINE_ENGINE_PARSER_OUTPUT_Q_RABBITMQ,
                    on_message_callback=callback,
                    auto_ack=False
                )
                channel.start_consuming()
            else:
                logger.info("Waiting for local_messages to be processed...")

        except KeyboardInterrupt:
            logger.info("User interrupted process")
            break

        except Exception as e:
            logger.exception("Lost connection to RabbitMQ. Retrying in 10 sec...")
            time.sleep(10)

            while True:
                try:
                    logger.info("Reconnecting to RabbitMQ...")
                    channel = create_rabbitmq_channel(AIRLINE_ENGINE_PARSER_OUTPUT_Q_RABBITMQ)
                    logger.info("Reconnected successfully!")
                    break
                except Exception as e:
                    logger.exception("Reconnection failed. Retrying in 10 sec...")
                    time.sleep(10)
