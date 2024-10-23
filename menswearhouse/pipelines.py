# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from menswearhouse.items import Store_cat_links_Item, Store_subcat_links_Item, storeLocatorLinks_Items, \
    Store_Details_Item


class MenswearhousePipeline:

    def __init__(self):
        # Initialize the pipeline and connect to MySQL database
        self.conn = pymysql.connect(
            host='localhost',         # Database host
            user='root',              # Database user
            password='actowiz',       # Database password
            database='mens_wearhouse_db'     # Database name
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):

        if isinstance(item,Store_cat_links_Item):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                              CREATE TABLE IF NOT EXISTS Store_cat_links (
                                                  `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                                  {columns_definitions}
                                              )
                                          """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM Store_cat_links")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE Store_cat_links ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO Store_cat_links ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()
                try:

                    update_query = "UPDATE states_store_locator SET status = 'Done' WHERE state = %s"
                    self.cursor.execute(update_query, (item.get('state'),))
                    self.conn.commit()


                except Exception as e:
                    print(f"Error updating states_store_locator: {e}")

            except Exception as e:
                print(f"Error inserting item into database: {e}")



        if isinstance(item, Store_subcat_links_Item):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                                     CREATE TABLE IF NOT EXISTS Store_subcat_links (
                                                         `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                                         {columns_definitions}
                                                     )
                                                 """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM Store_subcat_links")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE Store_subcat_links ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO Store_subcat_links ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")

        if isinstance(item, storeLocatorLinks_Items):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                                     CREATE TABLE IF NOT EXISTS storeLocatorLinks (
                                                         `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                                         {columns_definitions}
                                                     )
                                                 """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM storeLocatorLinks")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE storeLocatorLinks ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO storeLocatorLinks ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")

        if isinstance(item, Store_Details_Item):

            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join(
                    [f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields if field != 'unique_id'])

                # Create the table with columns matching item fields
                query = f"""
                            CREATE TABLE IF NOT EXISTS store_details (
                                                                `Store No.` INT AUTO_INCREMENT PRIMARY KEY,
                                                                 `unique_id` varchar(700) unique,

                                                                {columns_definitions}
                                                            )
                                                        """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM store_details")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE store_details ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")
            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO store_details ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()
                try:
                    # Update `subcat_of_cat_link` status
                    if 'unique_id' in item:
                        update_query = "UPDATE storelocatorlinks SET status = 'Done' WHERE unique_id = %s"
                        self.cursor.execute(update_query, (item['unique_id'],))
                        self.conn.commit()
                    else:
                        print("unique_id not found in item.")
                except Exception as e:
                    print(f"Error updating link: {e}")

            except Exception as e:
                print(f"Error inserting item into database: {e}")

            # try:
            #     # Update `subcat_of_cat_link` status
            #     if 'unique_id' in item:
            #         update_query = "UPDATE storelocatorlinks SET status = 'Done' WHERE unique_id = %s"
            #         self.cursor.execute(update_query, (item['unique_id'],))
            #         self.conn.commit()
            #     else:
            #         print("unique_id not found in item.")
            # except Exception as e:
            #     print(f"Error updating link: {e}")

        return item
