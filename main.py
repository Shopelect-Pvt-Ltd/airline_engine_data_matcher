import json
import logging
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
        host=PG_HOST,
        # host='10.200.20.73',
        database=PG_AIRLINES_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

# --- Create RabbitMQ channel ---
def create_rabbitmq_channel(queue_name):
    credentials = pika.PlainCredentials('finkraft', 'finkai@123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            # host='10.200.20.83',
            port=RabbitMQ_PORT,
            virtual_host='/',
            credentials=credentials
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    return channel


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


                print(f"Processing parsed record for airline: {airline}, doc_type: {doc_type}, pnr: {pnr}, ticket_number: {ticket_number}")
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
                print(f"Found invoice table data guid: {invoice_guid}, source: {invoice_source}, booking_guid: {booking_guid}, indigo_guid: {indigo_guid} , airline_name: {t_airline_name}")
                # --- Find matching record ---
                match = None
                target_table = None

                # If booking not found, try indigo_scraper_priority
                if indigo_guid :
                    print("----indigo_scraper_priority table----")
                    #add ticket no too if available
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
                    print("----indigo_scraper_priority table----")
                    #add ticket no too if available
                    # first search in priotity first 
                    cursor.execute("""
                        SELECT guid, transaction_type
                        FROM indigo_scraper_priority
                        WHERE  ticket/pnr=%s or 
                        ticket/pnr=%s or 
                    """, (ticket_number ,pnr))
                    match = cursor.fetchone()
                    if match:
                        target_table = "indigo_scraper_priority"
                        if not match:
                            logger.info(f"No matching booking/indigo found for PNR: {pnr}")

                # Try booking first
                elif booking_guid:
                    print("----booking table----")
                    cursor.execute("""
                        SELECT guid, za_data->>'Transaction_Type'
                        FROM airline_engine_booking
                        WHERE guid=%s
                    """, (booking_guid,))
                    match = cursor.fetchone()
                    if match:
                        target_table = "airline_engine_booking"

                elif not booking_guid and t_airline_name != 'indigo' and  ticket_number or pnr : 
                    cursor.execute("""
                        SELECT guid, za_data->>'Transaction_Type'
                        FROM airline_engine_booking
                        WHERE za_data->>'Ticket/PNR' = %s or  za_data->>'Ticket/PNR' = %s 
                    """, (ticket_number, pnr))
                    match = cursor.fetchone()
                    if match:
                        target_table = "airline_engine_booking"


                if match : 
                    print("match : ",match)
                    matched_guid, transaction_type = match
                else : 
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
                        insert_query = f"""
                                            INSERT INTO airline_zag_scraper_parser_matching_table (
                                                guid,
                                                airline_name,
                                                invoice_guid,
                                                invoice_status,
                                                parsed_at,
                                                matched_at,
                                                file_hash
                                            )
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (guid) DO NOTHING
                                        """
                        cursor.execute(insert_query, (guid,airline_name, invoice_guid, invoice_status,parsed_at,matched_at,invoice_filehash, matched_guid))
                        logger.info(f"✅ Updated {target_table} → {matched_guid} → {invoice_status}")

                    except Exception as e : 
                        logger.info(f"got error : {e}")

                # --- Determine invoice status ---
                invoice_status = None
                if transaction_type == "Invoice" and doc_type in ['INV','DB','BOS','BOS-DB']:
                    invoice_status = "invoice_received"
                elif transaction_type == "Refund" and doc_type in ['CR','BOS-CR']:
                    invoice_status = "refund_received"
                else:
                    logger.info(f"No matching status for {transaction_type}/{doc_type}")

                # --- Update correct table ---
                print("---updating in ",target_table,"----")
                update_query = f"""
                    UPDATE {target_table}
                    SET invoice_status=%s,
                        invoice_source=%s,
                        invoice_guid=%s,
                        invoice_file_hash=%s
                    WHERE guid=%s
                """
                cursor.execute(update_query, (invoice_status, invoice_source, invoice_guid, file_hash, matched_guid))
                logger.info(f"✅ Updated {target_table} → {matched_guid} → {invoice_status}")

            except Exception as e:
                logger.exception(f"Error processing record: {e}")
            
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
                insert_query = f"""
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
                                    ON CONFLICT (guid) DO NOTHING
                                """

                cursor.execute(insert_query, (matched_guid,airline_name, invoice_guid, invoice_status,parsed_at,matched_at,invoice_filehash))
                logger.info(f"✅ Updated {target_table} → {matched_guid} → {invoice_status}")

            except Exception as e :
                logger.info(f"Got error while inserting data into airline_za_scraper_parser_matching_table : {e}")

        # --- Process parsed data types ---
        if parsed_data1 and parsed_data2 and len(parsed_data1) != 0 and len(parsed_data2) != 0:
            logger.info("Processing tickets_data and amounts_data...")
            for ticket in parsed_data1:
                print("ticket : ",ticket)
                process_record(ticket)

        elif parsed_data_single and isinstance(parsed_data_single, list):
            logger.info("Processing single parsed_data list...")
            for ticket in parsed_data_single:
                print("ticket : ",ticket)
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
