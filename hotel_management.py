import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import time
import re
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

try:
    # Настройки подключения
    connection = psycopg2.connect(
        dbname='hotel_management',
        user='platonys',
        password='221205',
        host='localhost',
        port='5432'
    )

except Exception as e:
    print(f"Ошибка подключения: {e}")


class Database:
    def __init__(self, db_name=None):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def set_db_name(self, db_name):
        self.db_name = db_name

    def connect(self):
        if not self.db_name:
            return False
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def disconnect(self):
        if self.conn:
            try:
                self.conn.commit()  # commit before close
                os.sync()  # принудительная запись данных на диск
            except:
                pass
            self.conn.close()
            self.conn = None
            self.cursor = None

    def _execute_sql(self, sql, params=None):
        """Выполнение SQL запроса с обработкой исключений."""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            self.conn.commit()
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"SQL Execution Error: {e}")
            self.conn.rollback()  # откат изменений в случае ошибки
            return None

    def create_tables(self):
        """Создает таблицы базы данных"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS Hotels (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                City TEXT NOT NULL,
                Address TEXT NOT NULL,
                Rating REAL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Rooms (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                HotelId INTEGER NOT NULL,
                RoomType TEXT NOT NULL,
                PricePerNight REAL NOT NULL,
                MaxGuests INTEGER NOT NULL,
                FOREIGN KEY (HotelId) REFERENCES Hotels(Id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Bookings (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                HotelId INTEGER NOT NULL,
                GuestName TEXT NOT NULL,
                CheckInDate TEXT NOT NULL,
                CheckOutDate TEXT NOT NULL,
                TotalCost REAL DEFAULT 0,
                FOREIGN KEY (HotelId) REFERENCES Hotels(Id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS BookedRooms (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BookingId INTEGER NOT NULL,
                RoomId INTEGER NOT NULL,
                NumberOfNights INTEGER NOT NULL,
                FOREIGN KEY (BookingId) REFERENCES Bookings(Id),
                FOREIGN KEY (RoomId) REFERENCES Rooms(Id)
            )
            """
        ]
        for query in queries:
            self._execute_sql(query)

    def create_trigger(self):
        """Создает триггер для автоматического подсчета общей стоимости бронирования"""
        # Триггер для BookedRooms при вставке
        query = """
            CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnInsert
            AFTER INSERT ON BookedRooms
            BEGIN
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId = NEW.BookingId
                )
                WHERE Id = NEW.BookingId;
            END;
        """
        self._execute_sql(query)

        # Триггер для BookedRooms при обновлении
        query_update = """
           CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnUpdate
            AFTER UPDATE ON BookedRooms
            BEGIN
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId = NEW.BookingId
                )
                WHERE Id = NEW.BookingId;
            END;
        """
        self._execute_sql(query_update)

        # Триггер для BookedRooms при удалении
        query_delete = """
           CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnDelete
            AFTER DELETE ON BookedRooms
            BEGIN
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId = OLD.BookingId
                )
                WHERE Id = OLD.BookingId;
            END;
        """
        self._execute_sql(query_delete)
        # Триггер для Rooms при обновлении
        query_rooms_update = """
            CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnRoomsUpdate
            AFTER UPDATE ON Rooms
            BEGIN
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId IN (SELECT BookingId FROM BookedRooms WHERE RoomId = NEW.Id)
                )
                WHERE Id IN (SELECT BookingId FROM BookedRooms WHERE RoomId = NEW.Id);
            END;
        """
        self._execute_sql(query_rooms_update)

        # Триггер для Rooms при удалении
        query_rooms_delete = """
            CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnRoomsDelete
             AFTER DELETE ON Rooms
             BEGIN
                 UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId IN (SELECT BookingId FROM BookedRooms WHERE RoomId = OLD.Id)
                )
                WHERE Id IN (SELECT BookingId FROM BookedRooms WHERE RoomId = OLD.Id);
             END;
        """
        self._execute_sql(query_rooms_delete)

        # Триггер для Bookings при обновлении
        query_bookings_update = """
            CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnBookingsUpdate
            AFTER UPDATE ON Bookings
            BEGIN
               SELECT HotelBookingApp._recalculate_number_of_nights(NEW.Id);
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId = NEW.Id
                )
                WHERE Id = NEW.Id;
            END;
        """
        self._execute_sql(query_bookings_update)

        # Триггер для Bookings при удалении
        query_bookings_delete = """
            CREATE TRIGGER IF NOT EXISTS CalculateBookingCostOnBookingsDelete
            AFTER DELETE ON Bookings
            BEGIN
                UPDATE Bookings
                SET TotalCost = (
                    SELECT SUM(r.PricePerNight * br.NumberOfNights)
                    FROM Rooms r
                    JOIN BookedRooms br ON r.Id = br.RoomId
                    WHERE br.BookingId = OLD.Id
                )
                WHERE Id = OLD.Id;
            END;
        """
        self._execute_sql(query_bookings_delete)

    def _create_index(self, table, column):
        """Создает индекс для указанной колонки в таблице"""
        index_name = f"idx_{table}_{column}"
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({column})"
        self._execute_sql(sql)

    def create_indexes(self):
        self._create_index("Hotels", "Name")
        self._create_index("Rooms", "RoomType")
        self._create_index("Bookings", "GuestName")

    def clear_table(self, table, condition=None):
        """Очищает данные в таблице с условием или без"""
        sql = f"DELETE FROM {table}"
        if condition:
            sql += f" WHERE {condition}"
        self._execute_sql(sql)

    def insert_data(self, table, data):
        """Вставляет данные в таблицу"""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self._execute_sql(sql, tuple(data.values()))

    def update_data(self, table, data, condition):
        """Обновляет данные в таблице по условию"""
        set_clause = ", ".join([f"{key}=?" for key in data.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        self._execute_sql(sql, tuple(data.values()))

    def delete_data(self, table, condition):
        """Удаляет данные из таблицы по условию"""
        sql = f"DELETE FROM {table} WHERE {condition}"
        self._execute_sql(sql)

    def select_data(self, table, columns="*", condition=None):
        """Получает данные из таблицы"""
        sql = f"SELECT {columns} FROM {table}"
        if condition:
            sql += f" WHERE {condition}"
        return self._execute_sql(sql)

    def search_data(self, table, column, search_term):
        """Поиск данных по текстовому неключевому полю"""
        sql = f"SELECT * FROM {table} WHERE {column} LIKE ?"
        return self._execute_sql(sql, (f"%{search_term}%",))


class HotelBookingApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Приложение для бронирования отелей")
        self.root.geometry("1000x600")  # Увеличиваем размер окна
        self.db = Database()
        self.current_table = None
        self.is_db_open = False  # Флаг для отслеживания открыта ли база
        self.original_data = None
        self.search_results = None
        self.create_menu()
        self.disable_all_actions()  # изначально все кроме создать/открыть заблокировано
        self.input_validation = self.root.register(self.validate_input)
        self.reset_button = None
        self.table_label = tk.Label(self.root, text="", font=('Helvetica', 12, 'bold'))
        self.table_label.pack(pady=5)
        self.reset_button_frame = tk.Frame(self.root)  # Frame для кнопки сброса
        self.reset_button_frame.pack(pady=5)

    def create_menu(self):
        menu_bar = tk.Menu(self.root)
        self.root.config(menu=menu_bar)

        db_menu = tk.Menu(menu_bar, tearoff=0)
        db_menu.add_command(label="Создать базу данных", command=self.create_database)
        db_menu.add_command(label="Открыть базу данных", command=self.open_database)
        db_menu.add_command(label="Удалить базу данных", command=self.delete_database)
        menu_bar.add_cascade(label="База данных", menu=db_menu)

        self.table_menu = tk.Menu(menu_bar, tearoff=0)
        self.table_menu.add_command(label="Отели", command=lambda: self.show_table_data("Hotels"))
        self.table_menu.add_command(label="Номера", command=lambda: self.show_table_data("Rooms"))
        self.table_menu.add_command(label="Бронирования", command=lambda: self.show_table_data("Bookings"))
        self.table_menu.add_command(label="Забронированные номера", command=lambda: self.show_table_data("BookedRooms"))
        menu_bar.add_cascade(label="Таблицы", menu=self.table_menu)

        self.operations_menu = tk.Menu(menu_bar, tearoff=0)
        self.operations_menu.add_command(label="Показать все данные", command=self.show_all_data)
        self.operations_menu.add_command(label="Очистить таблицу", command=self.clear_table_data)
        self.operations_menu.add_command(label="Вставить данные", command=self.insert_data_window)
        self.operations_menu.add_command(label="Обновить данные", command=self.update_data_window)
        self.operations_menu.add_command(label="Удалить данные", command=self.delete_data_window)
        self.operations_menu.add_command(label="Поиск данных", command=self.search_data_window)
        self.operations_menu.add_command(label="Очистить все таблицы", command=self.clear_all_tables)
        menu_bar.add_cascade(label="Операции", menu=self.operations_menu)

    def enable_all_actions(self):
        """Включает все действия кроме создания и открытия БД."""
        self.is_db_open = True
        self.table_menu.entryconfig("Отели", state="normal")
        self.table_menu.entryconfig("Номера", state="normal")
        self.table_menu.entryconfig("Бронирования", state="normal")
        self.table_menu.entryconfig("Забронированные номера", state="normal")

        self.operations_menu.entryconfig("Показать все данные", state="normal")
        self.operations_menu.entryconfig("Очистить таблицу", state="normal")
        self.operations_menu.entryconfig("Вставить данные", state="normal")
        self.operations_menu.entryconfig("Обновить данные", state="normal")
        self.operations_menu.entryconfig("Удалить данные", state="normal")
        self.operations_menu.entryconfig("Поиск данных", state="normal")
        self.operations_menu.entryconfig("Очистить все таблицы", state="normal")

    def disable_all_actions(self):
        """Отключает все действия, кроме создания и открытия БД."""
        self.is_db_open = False
        self.table_menu.entryconfig("Отели", state="disabled")
        self.table_menu.entryconfig("Номера", state="disabled")
        self.table_menu.entryconfig("Бронирования", state="disabled")
        self.table_menu.entryconfig("Забронированные номера", state="disabled")

        self.operations_menu.entryconfig("Показать все данные", state="disabled")
        self.operations_menu.entryconfig("Очистить таблицу", state="disabled")
        self.operations_menu.entryconfig("Вставить данные", state="disabled")
        self.operations_menu.entryconfig("Обновить данные", state="disabled")
        self.operations_menu.entryconfig("Удалить данные", state="disabled")
        self.operations_menu.entryconfig("Поиск данных", state="disabled")
        self.operations_menu.entryconfig("Очистить все таблицы", state="disabled")

    def create_database(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".db", filetypes=[("Database files", "*.db")])
        if file_path:
            self.db.set_db_name(file_path)
            if self.db.connect():
                self.db.create_tables()
                self.db.create_trigger()
                self.db.create_indexes()
                self.db.disconnect()
                self.enable_all_actions()
                messagebox.showinfo("Успех", "База данных создана успешно!")
                self.show_table_data("Hotels")  # Открыть таблицу отели
            else:
                messagebox.showerror("Ошибка", "Не удалось создать базу данных!")

    def open_database(self):
        file_path = filedialog.askopenfilename(defaultextension=".db", filetypes=[("Database files", "*.db")])
        if file_path:
            if self.db.conn:
                self.db.disconnect()
            self.db.set_db_name(file_path)
            if self.db.connect():
                self.enable_all_actions()
                messagebox.showinfo("Успех", "База данных открыта успешно!")
                self.show_table_data("Hotels")
            else:
                messagebox.showerror("Ошибка", "Не удалось открыть базу данных!")

    def delete_database(self):
        file_path = filedialog.askopenfilename(defaultextension=".db", filetypes=[("Database files", "*.db")])
        if file_path:
            if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить выбранную базу данных?"):
                try:

                    db_to_delete = Database(file_path)
                    if db_to_delete.connect():
                        for _ in range(3):  # несколько попыток закрытия
                            db_to_delete.disconnect()
                            time.sleep(0.1)
                            if not db_to_delete.conn:
                                break

                    if os.path.exists(file_path):
                        os.remove(file_path)
                        if self.db.db_name == file_path:
                            self.disable_all_actions()
                            self.clear_treeview()
                            self.db.set_db_name(None)
                        messagebox.showinfo("Успех", "База данных удалена успешно!")

                    else:
                        messagebox.showinfo("Информация", "База данных не найдена.")

                except FileNotFoundError:
                    messagebox.showerror("Ошибка", "Файл базы данных не найден!")
                except Exception as e:
                    messagebox.showerror("Ошибка", f"Не удалось удалить базу данных: {e}")

    def show_table_data(self, table):
        self.current_table = table
        if self.db.connect():
            data = self.db.select_data(table)
            display_columns, db_columns = self.get_table_columns(table)
            self.original_data = data
            self.table_label.config(text=self.get_translated_table_name(table))
            self.show_data_in_tree(data, display_columns, self.root)  # передаю root
            self.db.disconnect()
        else:
            messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

    def _recalculate_number_of_nights(self, booking_id):
        """Пересчитывает количество ночей и обновляет записи в BookedRooms"""
        if self.db.connect():
            sql = f"SELECT CheckInDate, CheckOutDate FROM Bookings WHERE Id = ?"
            result = self.db._execute_sql(sql, (booking_id,))
            if result and result[0]:
                check_in_date_str, check_out_date_str = result[0]
                check_in = datetime.strptime(check_in_date_str, '%d.%m.%Y')
                check_out = datetime.strptime(check_out_date_str, '%d.%m.%Y')
                number_of_nights = (check_out - check_in).days
                update_sql = f"UPDATE BookedRooms SET NumberOfNights = ? WHERE BookingId = ?"
                self.db._execute_sql(update_sql, (number_of_nights, booking_id))

            else:
                messagebox.showerror("Ошибка", "Не удалось получить даты бронирования.")
                return False
        else:
            messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных.")
            return False
        return True

    def get_translated_table_name(self, table):
        translations = {
            "Hotels": "Отели",
            "Rooms": "Номера",
            "Bookings": "Бронирования",
            "BookedRooms": "Забронированные номера"
        }
        return translations.get(table, table)

    def show_all_data(self):
        if self.db.connect():
            data = {}
            for table in ["Hotels", "Rooms", "Bookings", "BookedRooms"]:
                data[table] = self.db.select_data(table)
            self.db.disconnect()
            self.show_all_data_in_tabs(data)
        else:
            messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

    def get_table_columns(self, table):
        if self.db.connect():
            self.db.cursor.execute(f"PRAGMA table_info({table})")
            columns = [column[1] for column in self.db.cursor.fetchall()]
            self.db.disconnect()

            translations = {
                "Name": "Название",
                "City": "Город",
                "Address": "Адрес",
                "Rating": "Рейтинг",
                "HotelId": "ID отеля",
                "RoomType": "Тип номера",
                "PricePerNight": "Цена за ночь",
                "MaxGuests": "Макс гостей",
                "GuestName": "Имя гостя",
                "CheckInDate": "Дата заезда",
                "CheckOutDate": "Дата выезда",
                "TotalCost": "Общая стоимость",
                "NumberOfNights": "Количество ночей",
                "BookingId": "ID бронирования",
                "RoomId": "ID номера"

            }

            display_columns = [translations.get(col, col) for col in columns]  # перевод, если есть, иначе как есть
            display_columns = [col.replace('_', ' ') for col in display_columns]
            return display_columns, columns  # возвращаем кортеж: отображаемые и реальные имена столбцов
        else:
            return [], []

    def clear_table_data(self):
        if not self.current_table:
            messagebox.showinfo("Информация", "Пожалуйста, сначала выберите таблицу.")
            return
        if self.db.connect():
            if messagebox.askyesno("Подтверждение",
                                   f"Вы уверены, что хотите очистить таблицу {self.get_translated_table_name(self.current_table)}?"):
                self.db.clear_table(self.current_table)
                self.show_table_data(self.current_table)
            self.db.disconnect()
        else:
            messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

    def show_all_data_in_tabs(self, data):
        """Отображает все данные из всех таблиц в разных вкладках"""
        if data is None:
            messagebox.showerror("Ошибка", "Нет данных для отображения")
            return

        window = tk.Toplevel(self.root)
        window.title("Все данные")
        tabControl = ttk.Notebook(window)
        tabControl.pack(expand=1, fill="both")

        translations = {
            "Hotels": "Отели",
            "Rooms": "Номера",
            "Bookings": "Бронирования",
            "BookedRooms": "Забронированные номера"
        }

        for table, rows in data.items():
            frame = ttk.Frame(tabControl)
            tabControl.add(frame, text=translations.get(table, table))
            display_columns, _ = self.get_table_columns(table)
            self.show_data_in_tree(rows, display_columns, frame)

    def show_data_in_tree(self, data, columns, parent=None, table_name=None):
        """Отображает данные в виджете Treeview"""
        if parent is None:
            parent = self.root
            for child in parent.winfo_children():
                if isinstance(child, ttk.Treeview):
                    child.destroy()
        else:
            for child in parent.winfo_children():
                if isinstance(child, ttk.Treeview):
                    child.destroy()

        tree = ttk.Treeview(parent, columns=columns, show='headings')
        for column in columns:
            tree.heading(column, text=column)

        for row in data:
            tree.insert('', 'end', values=row)

        tree.pack(expand=1, fill="both")

        if self.search_results and parent == self.root:
            if not self.reset_button:
                self.reset_button = tk.Button(self.reset_button_frame, text="Сбросить поиск", command=self.reset_search)
                self.reset_button.pack(pady=5)

        return tree

    def clear_treeview(self):
        """Очищает все виджеты treeview"""
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Treeview):
                child.destroy()

    def validate_input(self, new_value, reason):
        """Проверяет, что в поле ввода только цифры"""
        if reason == 'focusin' or reason == 'focusout' or new_value == "":
            return True  # Разрешаем фокус и пустое поле
        return re.match(r'^\d*$', new_value) is not None

    def validate_date(self, date_text):
        try:
            datetime.strptime(date_text, '%d.%m.%Y')
            return True
        except ValueError:
            return False

    def insert_data_window(self):
        """Окно для вставки данных"""
        if not self.current_table:
            messagebox.showinfo("Информация", "Пожалуйста, сначала выберите таблицу.")
            return

        window = tk.Toplevel(self.root)
        window.title(f"Вставить данные в {self.get_translated_table_name(self.current_table)}")
        display_columns, db_columns = self.get_table_columns(self.current_table)
        labels, entries = [], []

        # get next id
        next_id = self._get_next_id(self.current_table)

        for i, column in enumerate(display_columns):
            if column == "Общая стоимость" or column == "Количество ночей":
                continue
            tk.Label(window, text=column).grid(row=i, column=0, padx=5, pady=5)
            if column == "Id":
                entry = tk.Entry(window, validate="key", validatecommand=(self.input_validation, '%P', '%V'))
                entry.insert(0, next_id)  # fill with next id
            elif column in ["Дата заезда", "Дата выезда"]:
                entry = tk.Entry(window)
            elif column == "Количество ночей" or column == "Общая стоимость":
                continue
            else:
                entry = tk.Entry(window)
            entry.grid(row=i, column=1, padx=5, pady=5)
            labels.append(db_columns[i])  # Используем db_columns для записи в БД
            entries.append(entry)

        def insert_action():
            data = {}
            for i, label in enumerate(labels):
                data[label] = entries[i].get()

            if self.current_table == "Bookings":
                check_in_date = data.get("CheckInDate")
                check_out_date = data.get("CheckOutDate")
                if not (check_in_date and check_out_date):
                    messagebox.showerror("Ошибка", "Необходимо ввести дату заезда и выезда.")
                    return
                if not (self.validate_date(check_in_date) and self.validate_date(check_out_date)):
                    messagebox.showerror("Ошибка", "Неверный формат даты")
                    return

                check_in = datetime.strptime(check_in_date, '%d.%m.%Y')
                check_out = datetime.strptime(check_out_date, '%d.%m.%Y')
                if check_out <= check_in:
                    messagebox.showerror("Ошибка", "Дата выезда должна быть больше даты заезда хотя бы на 1 день.")
                    return
                data["TotalCost"] = 0  # Default total cost for Bookings

            elif self.current_table == "BookedRooms":
                if not self._check_foreign_keys(self.current_table, data):
                    return
                booking_id = data.get("BookingId")
                if self.db.connect():
                    sql = f"SELECT CheckInDate, CheckOutDate FROM Bookings WHERE Id = ?"
                    result = self.db._execute_sql(sql, (booking_id,))
                    self.db.disconnect()
                    if result and result[0]:
                        check_in_date_str, check_out_date_str = result[0]
                        check_in = datetime.strptime(check_in_date_str, '%d.%m.%Y')
                        check_out = datetime.strptime(check_out_date_str, '%d.%m.%Y')
                        number_of_nights = (check_out - check_in).days
                        data["NumberOfNights"] = number_of_nights

                    else:
                        messagebox.showerror("Ошибка", "Не удалось получить даты бронирования.")
                        return
            if not self._check_foreign_keys(self.current_table, data):
                return  # Exit if foreign key check fails

            if not self._check_id_not_exists(self.current_table, data.get("Id")):
                return  # Exit if ID exists

            if self.db.connect():
                self.db.insert_data(self.current_table, data)

                if self.current_table == "Bookings":
                    if self._recalculate_number_of_nights(data.get("Id")):
                        self.db.disconnect()
                    else:
                        self.db.disconnect()
                        return

                self.db.disconnect()

                self.show_table_data(self.current_table)
                window.destroy()
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

        tk.Button(window, text="Вставить", command=insert_action).grid(row=len(labels), column=0, columnspan=2, padx=5,
                                                                       pady=10)

    def _check_id_not_exists(self, table, record_id):
        if not record_id:
            return True
        if self.db.connect():
            sql = f"SELECT COUNT(*) FROM {table} WHERE Id=?"
            result = self.db._execute_sql(sql, (record_id,))
            self.db.disconnect()
            if result and result[0][0] > 0:
                messagebox.showerror("Ошибка", "Введённый ID уже занят")
                return False
        return True

    def _check_foreign_keys(self, table, data):
        if table == "Rooms":
            if not self._check_id_exists("Hotels", data.get("HotelId")):
                messagebox.showerror("Ошибка", "Введённый ID отеля не существует")
                return False
        elif table == "Bookings":
            if not self._check_id_exists("Hotels", data.get("HotelId")):
                messagebox.showerror("Ошибка", "Введённый ID отеля не существует")
                return False
        elif table == "BookedRooms":
            if not self._check_id_exists("Bookings", data.get("BookingId")):
                messagebox.showerror("Ошибка", "Введённый ID бронирования не существует")
                return False
            if not self._check_id_exists("Rooms", data.get("RoomId")):
                messagebox.showerror("Ошибка", "Введённый ID номера не существует")
                return False

        return True

    def _check_id_exists(self, table, record_id):
        if not record_id:
            return True
        if self.db.connect():
            sql = f"SELECT COUNT(*) FROM {table} WHERE Id=?"
            result = self.db._execute_sql(sql, (record_id,))
            self.db.disconnect()
            if result and result[0][0] > 0:
                return True
        return False

    def _get_next_id(self, table):
        if self.db.connect():
            sql = f"SELECT MAX(Id) FROM {table}"
            result = self.db._execute_sql(sql)
            self.db.disconnect()
            if result and result[0][0] is not None:
                return result[0][0] + 1
            else:
                return 1
        return 1

    def update_data_window(self):
        """Окно для обновления данных"""
        if not self.current_table:
            messagebox.showinfo("Информация", "Пожалуйста, сначала выберите таблицу.")
            return

        window = tk.Toplevel(self.root)
        window.title(f"Обновить данные в {self.get_translated_table_name(self.current_table)}")
        display_columns, db_columns = self.get_table_columns(self.current_table)
        labels, entries = [], []

        tk.Label(window, text="ID записи:").grid(row=0, column=0, padx=5, pady=5)
        id_entry = tk.Entry(window, validate="key", validatecommand=(self.input_validation, '%P', '%V'))
        id_entry.grid(row=0, column=1, padx=5, pady=5)

        for i, column in enumerate(display_columns[1:]):
            if column == "Общая стоимость" or column == "Количество ночей":
                continue
            tk.Label(window, text=column).grid(row=i + 1, column=0, padx=5, pady=5)
            entry = tk.Entry(window)
            entry.grid(row=i + 1, column=1, padx=5, pady=5)
            labels.append(db_columns[i + 1])  # Используем db_columns для записи в БД
            entries.append(entry)

        def update_action():
            record_id = id_entry.get()
            data = {}
            for i, label in enumerate(labels):
                data[label] = entries[i].get()

            if not self._check_id_exists(self.current_table, record_id):
                messagebox.showerror("Ошибка", "Введённый ID не существует")
                return

            if self.current_table == "Bookings":
                check_in_date = data.get("CheckInDate")
                check_out_date = data.get("CheckOutDate")

                if check_in_date and check_out_date:
                    if not (self.validate_date(check_in_date) and self.validate_date(check_out_date)):
                        messagebox.showerror("Ошибка", "Неверный формат даты")
                        return

                    check_in = datetime.strptime(check_in_date, '%d.%m.%Y')
                    check_out = datetime.strptime(check_out_date, '%d.%m.%Y')
                    if check_out <= check_in:
                        messagebox.showerror("Ошибка", "Дата выезда должна быть больше даты заезда хотя бы на 1 день.")
                        return

                    if self.db.connect():
                        condition = f"Id={record_id}"
                        self.db.update_data(self.current_table, data, condition)

                        if self._recalculate_number_of_nights(record_id):
                            self.db.disconnect()
                        else:
                            self.db.disconnect()
                            return
                        self.db.disconnect()

                    else:
                        messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")
                        return
                else:
                    if self.db.connect():
                        condition = f"Id={record_id}"
                        self.db.update_data(self.current_table, data, condition)
                        self.db.disconnect()

                    else:
                        messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")
                        return

            elif self.current_table == "BookedRooms":
                if self.db.connect():
                    condition = f"Id={record_id}"
                    self.db.update_data(self.current_table, data, condition)
                    self.db.disconnect()
                else:
                    messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")
                    return


            else:
                if self.db.connect():
                    condition = f"Id={record_id}"
                    self.db.update_data(self.current_table, data, condition)
                    self.db.disconnect()
                else:
                    messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")
                    return

            self.show_table_data(self.current_table)
            window.destroy()

        tk.Button(window, text="Обновить", command=update_action).grid(row=len(labels) + 2, column=0, columnspan=2,
                                                                       padx=5, pady=10)

    def delete_data_window(self):
        """Окно для удаления данных"""
        if not self.current_table:
            messagebox.showinfo("Информация", "Пожалуйста, сначала выберите таблицу.")
            return
        window = tk.Toplevel(self.root)
        window.title(f"Удалить данные из {self.get_translated_table_name(self.current_table)}")
        display_columns, db_columns = self.get_table_columns(self.current_table)

        tk.Label(window, text="Выбрать столбец:").grid(row=0, column=0, padx=5, pady=5)
        column_var = tk.StringVar(window)
        column_var.set(display_columns[1])  # select second column by default
        column_dropdown = ttk.Combobox(window, textvariable=column_var, values=display_columns)
        column_dropdown.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(window, text="Значение:").grid(row=1, column=0, padx=5, pady=5)
        condition_entry = tk.Entry(window)
        condition_entry.grid(row=1, column=1, padx=5, pady=5)

        def delete_action():
            selected_column_index = display_columns.index(column_var.get())
            column = db_columns[selected_column_index]
            condition = f"{column} = '{condition_entry.get()}'"
            if self.db.connect():
                if messagebox.askyesno("Подтверждение", f"Вы уверены, что хотите удалить записи?"):
                    booking_ids = []
                    if self.current_table == "Bookings":
                        sql = f"SELECT Id from Bookings where {condition}"
                        result = self.db._execute_sql(sql)
                        if result:
                            booking_ids = [row[0] for row in result]
                    elif self.current_table == "BookedRooms":
                        sql = f"SELECT BookingId from BookedRooms where {condition}"
                        result = self.db._execute_sql(sql)
                        if result:
                            booking_ids = [row[0] for row in result]
                    elif self.current_table == "Rooms":
                        sql = f"SELECT br.BookingId FROM BookedRooms br JOIN Rooms r ON br.RoomId = r.Id where {condition}"
                        result = self.db._execute_sql(sql)
                        if result:
                            booking_ids = [row[0] for row in result]

                    self.db.delete_data(self.current_table, condition)
                    if booking_ids:
                        for booking_id in booking_ids:
                            if self._recalculate_number_of_nights(booking_id):
                                continue
                            else:
                                self.db.disconnect()
                                return
                    self.show_table_data(self.current_table)
                    self.db.disconnect()
                    window.destroy()
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

        tk.Button(window, text="Удалить", command=delete_action).grid(row=2, column=0, columnspan=2, padx=5, pady=10)

    def search_data_window(self):
        """Окно для поиска данных"""
        if not self.current_table:
            messagebox.showinfo("Информация", "Пожалуйста, сначала выберите таблицу.")
            return
        window = tk.Toplevel(self.root)
        window.title(f"Поиск данных в {self.get_translated_table_name(self.current_table)}")
        display_columns, db_columns = self.get_table_columns(self.current_table)

        tk.Label(window, text="Искать по столбцу:").grid(row=0, column=0, padx=5, pady=5)
        column_var = tk.StringVar(window)
        column_var.set(display_columns[1])  # Выбираем второй столбец по умолчанию
        column_dropdown = ttk.Combobox(window, textvariable=column_var, values=display_columns)
        column_dropdown.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(window, text="Поисковый запрос:").grid(row=1, column=0, padx=5, pady=5)
        search_entry = tk.Entry(window)
        search_entry.grid(row=1, column=1, padx=5, pady=5)

        def search_action():
            selected_column_index = display_columns.index(column_var.get())
            column = db_columns[selected_column_index]
            search_term = search_entry.get()
            if self.db.connect():
                data = self.db.search_data(self.current_table, column, search_term)
                self.search_results = data
                self.show_data_in_tree(data, display_columns, self.root)
                self.db.disconnect()
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

        tk.Button(window, text="Поиск", command=search_action).grid(row=2, column=0, columnspan=2, padx=5, pady=10)

    def reset_search(self):
        if self.original_data:
            if self.reset_button:
                self.reset_button.destroy()
                self.reset_button = None
            self.search_results = None
            display_columns, _ = self.get_table_columns(self.current_table)
            sorted_data = sorted(self.original_data, key=lambda row: row[0])  # Сортируем по id
            self.show_data_in_tree(sorted_data, display_columns, self.root)

    def clear_all_tables(self):
        if self.db.connect():
            if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите очистить все таблицы?"):
                for table in ["Hotels", "Rooms", "Bookings", "BookedRooms"]:
                    self.db.clear_table(table)
                messagebox.showinfo("Успех", "Все таблицы очищены")
            self.db.disconnect()
        else:
            messagebox.showerror("Ошибка", "Не удалось подключиться к базе данных!")

if __name__ == "__main__":
    root = tk.Tk()
    app = HotelBookingApp(root)
    root.mainloop()