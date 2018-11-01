#!/usr/bin/env python3
"""
DeviceManagement Database module.
Contains all interactions between the webapp and the queries to the database.
"""

import configparser
import datetime
from typing import List, Optional

import setup_vendor_path  # noqa
import pg8000

################################################################################
#   Welcome to the database file, where all the query magic happens.
#   My biggest tip is look at the *week 9 lab*.
#   Important information:
#       - If you're getting issues and getting locked out of your database.
#           You may have reached the maximum number of connections.
#           Why? (You're not closing things!) Be careful!
#       - Check things *carefully*.
#       - There may be better ways to do things, this is just for example
#           purposes
#       - ORDERING MATTERS
#           - Unfortunately to make it easier for everyone, we have to ask that
#               your columns are in order. WATCH YOUR SELECTS!! :)
#   Good luck!
#       And remember to have some fun :D
################################################################################


#####################################################
#   Database Connect
#   (No need to touch
#       (unless the exception is potatoing))
#####################################################

def database_connect():
    """
    Connects to the database using the connection string.
    If 'None' was returned it means there was an issue connecting to
    the database. It would be wise to handle this ;)
    """
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    if 'database' not in config['DATABASE']:
        config['DATABASE']['database'] = config['DATABASE']['user']

    # Create a connection to the database
    connection = None
    try:
        # Parses the config file and connects using the connect string
        connection = pg8000.connect(database=config['DATABASE']['database'],
                                    user=config['DATABASE']['user'],
                                    password=config['DATABASE']['password'],
                                    host=config['DATABASE']['host'])
    except pg8000.OperationalError as operation_error:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(operation_error)
        return None

    # return the connection to use
    return connection


#####################################################
#   Query (a + a[i])
#   Login
#####################################################

def check_login(employee_id, password: int) -> Optional[dict]:
    """
    Check that the users information exists in the database.
        - True => return the user data
        - False => return None
    """

    # Note: this example system is not well-designed for security.
    # There are several serious problems. One is that the database
    # stores passwords directly; a better design would "salt" each password
    # and then hash the result, and store only the hash.
    # This is ok for a toy assignment, but do not use this code as a model when you are
    # writing a real system for a client or yourself.

    # TODO
    # Check if the user details are correct!
    # Return the relevant information (watch the order!)

    # TODO Dummy data - change rows to be useful!
    # NOTE: Make sure you take care of ORDER!!!

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT * \
        FROM Employee \
        WHERE empid=%s and password=%s", [employee_id, password])
    except:
    # This happens if there is an error executing the query
        print("Error executing function")
    results = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection
    if len(results) != 0:
    # TODO Dummy data - change rows to be useful!
    # NOTE: Make sure you take care of ORDER!!!
    #    dates = results[0][3].split()
        employee_info = [
            results[0][0],         # empid
            results[0][1],         # name
            results[0][2],          # homeAddress
            results[0][3],  # dateOfBirth
        ]
    else :
        return None

    user = {
        'empid': employee_info[0],
        'name': employee_info[1],
        'homeAddress': employee_info[2],
        'dateOfBirth': employee_info[3],
    }

    return user


#####################################################
#   Query (f[i])
#   Is Manager?
#####################################################

def is_manager(employee_id: int) -> Optional[str]:
    """
    Get the department the employee is a manager of, if any.
    Returns None if the employee doesn't manage a department.
    """

    # TODO Dummy Data - Change to be useful!
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT name \
        FROM Department \
        WHERE manager=%s", [employee_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing function")
    results = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection
    if len(results) !=0 :
        manager_of = results[0][0]
    else :
        manager_of = None

    return manager_of


#####################################################
#   Query (a[ii])
#   Get My Used Devices
#####################################################

def get_devices_used_by(employee_id: int) -> list:
    """
    Get a list of all the devices used by the employee.
    """

    # TODO Dummy Data - Change to be useful!
    # Return a list of devices issued to the user!
    # Each "Row" contains [ deviceID, manufacturer, modelNumber]
    # If no devices = empty list []

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT deviceid, manufacturer, modelnumber \
        FROM Device inner join DeviceUsedBy using (deviceid) \
        WHERE empid=%s", [employee_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing function")
    devices = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    return devices


#####################################################
#   Query (a[iii])
#   Get departments employee works in
#####################################################

def employee_works_in(employee_id: int) -> List[str]:
    """
    Return the departments that the employee works in.
    """

    # TODO Dummy Data - Change to be useful!
    # Return a list of departments

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT department \
        FROM EmployeeDepartments \
        WHERE empid=%s", [employee_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing function")
    results = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection
    departments = []
    for i in range(len(results)):
        departments.append(results[i][0])
    return departments


#####################################################
#   Query (c)
#   Get My Issued Devices
#####################################################

def get_issued_devices_for_user(employee_id: int) -> list:
    """
    Get all devices issued to the user.
        - Return a list of all devices to the user.
    """

    # TODO Dummy Data - Change to be useful!
    # Return a list of devices issued to the user!
    # Each "Row" contains [ deviceID, purchaseDate, manufacturer, modelNumber ]
    # If no devices = empty list []

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT deviceid, purchasedate, manufacturer, modelnumber \
        FROM Device \
        WHERE issuedto=%s", [employee_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_issued_devices_for_user")
    devices = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    return devices


#####################################################
#   Query (b)
#   Get All Models
#####################################################

def get_all_models() -> list:
    """
    Get all models available.
    """

    # TODO Dummy Data - Change to be useful!
    # Return the list of models with information from the model table.
    # Each "Row" contains: [manufacturer, description, modelnumber, weight]
    # If No Models = EMPTY LIST []
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        cursor.execute("select *\
                        from model")
    except:
        print("Error executing get_all_models.")

    r = cursor.fetchall()
    cursor.close()
    connection.close()
    if r != None:
        return r
    else:
        return None


#####################################################
#   Query (d[ii])
#   Get Device Repairs
#####################################################

def get_device_repairs(device_id: int) -> list:
    """
    Get all repairs made to a device.
    """

    # TODO Dummy Data - Change to be useful!
    # Return the repairs done to a certain device
    # Each "Row" contains:
    #       - repairid
    #       - faultreport
    #       - startdate
    #       - enddate
    #       - cost
    # If no repairs = empty list

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT repairid, faultreport, startdate, enddate, cost\
         FROM Repair\
        WHERE doneto=%s", [device_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_device_repairs")
    repairs = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    return repairs


#####################################################
#   Query (d[i])
#   Get Device Info
#####################################################

def get_device_information(device_id: int) -> Optional[dict]:
    """
    Get related device information in detail.
    """

    # TODO Dummy Data - Change to be useful!
    # Return all the relevant device information for the device
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT *\
         FROM Device\
         WHERE deviceid=%s", [device_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_device_information")
    device_info = cursor.fetchone()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    device = {
        'device_id': device_info[0],
        'serial_number': device_info[1],
        'purchase_date': device_info[2],
        'purchase_cost': device_info[3],
        'manufacturer': device_info[4],
        'model_number': device_info[5],
        'issued_to': device_info[6],
    }

    return device


#####################################################
#   Query (d[iii/iv])
#   Get Model Info by Device
#####################################################

def get_device_model(device_id: int) -> Optional[dict]:
    """
    Get model information about a device.
    """

    # TODO Dummy Data - Change to be useful!
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT manufacturer, modelNumber, description, weight\
        FROM model JOIN device USING (manufacturer, modelnumber)\
        WHERE deviceid=%s", [device_id])
    except:
        print("Error executing get_device_model")
    results = cursor.fetchall()
    if len(results) == 0:
        print("get_device_model: cannot find results")
        return None
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection
    model_info = [
        results[0][0],  # manufacturer
        results[0][1],  # modelNumber
        results[0][2],  # description
        results[0][3],  # weight
    ]

    model = {
        'manufacturer': model_info[0],
        'model_number': model_info[1],
        'description': model_info[2],
        'weight': model_info[3],
    }
    return model


#####################################################
#   Query (e)
#   Get Repair Details
#####################################################

def get_repair_details(repair_id: int) -> Optional[dict]:
    """
    Get information about a repair in detail, including service information.
    """

    # TODO Dummy data - Change to be useful!

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT repairid, faultreport, startdate, enddate, cost, doneby, servicename, email, doneto \
        FROM Repair JOIN Service ON (doneby = abn) \
        WHERE repairid=%s", [repair_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_repair_details")
    repair_info = cursor.fetchone()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    repair = {
        'repair_id': repair_info[0],
        'fault_report': repair_info[1],
        'start_date': repair_info[2],
        'end_date': repair_info[3],
        'cost': repair_info[4],
        'done_by': {
            'abn': repair_info[5],
            'service_name': repair_info[6],
            'email': repair_info[7],
        },
        'done_to': repair_info[8],
    }
    return repair


#####################################################
#   Query (f[ii])
#   Get Models assigned to Department
#####################################################

def get_department_models(department_name: str) -> list:
    """
    Return all models assigned to a department.
    """

    # TODO Dummy Data - Change to be useful!
    # Return the models allocated to the department.
    # Each "row" has: [ manufacturer, modelnumber, maxnumber ]
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT manufacturer, modelnumber, maxnumber \
        FROM modelallocations \
        WHERE department=%s", [department_name])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_department_models")
    model_allocations = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    return model_allocations


#####################################################
#   Query (f[iii])
#   Get Number of Devices of Model owned
#   by Employee in Department
#####################################################

def get_employee_department_model_device(department_name: str, manufacturer: str, model_number: str) -> list:
    """
    Get the number of devices owned per employee in a department
    matching the model.

    E.g. Model = iPhone, Manufacturer = Apple, Department = "Accounting"
        - [ 1337, Misty, 20 ]
        - [ 351, Pikachu, 10 ]
    """

    # TODO Dummy Data - Change to be useful!
    # Return the number of devices owned by each employee matching department,
    #   manufacturer and model.
    # Each "row" has: [ empid, name, number of devices issued that match ]

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("\
        SELECT issuedto AS empid, name, COUNT(deviceid) \
        FROM (Device JOIN model USING (modelnumber, manufacturer)) \
            JOIN modelallocations USING (modelnumber, manufacturer), \
            employee e1 JOIN employeedepartments e2 ON (e1.empid = e2.empid)\
        WHERE e2.department=%s AND manufacturer=%s AND modelnumber=%s AND e1.empid=issuedto\
        GROUP BY issuedto, name", [department_name, manufacturer,\
        model_number])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_employee_department_model_device")
    employee_counts = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    return employee_counts


#####################################################
#   Query (f[iv])
#   Get a list of devices for a certain model and
#       have a boolean showing if the employee has
#       it issued.
#####################################################

def get_model_device_assigned(model_number: str, manufacturer: str, employee_id: int) -> list:
    """
    Get all devices matching the model and manufacturer and show True/False
    if the employee has the device assigned.

    E.g. Model = Pixel 2, Manufacturer = Google, employee_id = 1337
        - [123656, False]
        - [123132, True]
        - [51413, True]
        - [8765, False]
    """

    # TODO Dummy Data - Change to be useful!
    # Return each device of this model and whether the employee has it
    # issued.
    # Each "row" has: [ device_id, True if issued, else False.]

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT deviceid, issuedto \
        FROM Device \
        WHERE manufacturer=%s and modelnumber=%s",[manufacturer, model_number])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_model_device_assigned")
    results = cursor.fetchall()
    cursor.close() # IMPORTANT: close cursor
    connection.close() # IMPORTANT: close connection

    device_assigned = []
    if results != None:
        for i in range(len(results)):
            if results[i][1] == int(employee_id):
                row = [results[i][0], True]
            else:
                row = [results[i][0], False]
            device_assigned.append(row)
    else :
        return None


    return device_assigned


#####################################################
#   Get a list of devices for this model and
#       manufacturer that have not been assigned.
#####################################################

def get_unassigned_devices_for_model(model_number: str, manufacturer: str) -> list:
    """
    Get all unassigned devices for the model.
    """
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT deviceid, issuedto \
        FROM device \
        WHERE modelnumber=%s AND manufacturer=%s", [model_number, manufacturer])
    except Exception as e:
        print("Error executing get_unassigned_devices_for_model: {}".format(e))
    results=cursor.fetchall()
    cursor.close()
    connection.close()
    out=[]
    for i in range(len(results)):
        if results[i][1] == None:
            out.append(results[i][0])
    # TODO Dummy Data - Change to be useful!
    # Return each device of this model that has not been issued
    # Each "row" has: [ device_id ]
    # device_unissued = [123656, 123132, 51413, 8765]
    return out


#####################################################
#   Get Employees in Department
#####################################################

def get_employees_in_department(department_name: str) -> list:
    """
    Return all the employees' IDs and names in a given department.
    """

    # TODO Dummy Data - Change to be useful!
    # Return the employees in the department.
    # Each "row" has: [ empid, name ]
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT empid, name\
        FROM employeedepartments JOIN employee USING (empid)\
        WHERE department=%s",[department_name])
    except:
    # This happens if there is an error executing the query
        print("Error executing get_employees_in_department")
    employees = cursor.fetchall()
    cursor.close()
    connection.close()

    return employees


#####################################################
#   Query (f[v])
#   Issue Device
#####################################################

def issue_device_to_employee(employee_id: int, device_id: int):
    """
    Issue the device to the chosen employee.
    """

    # TODO issue the device from the employee
    # Return (True, None) if all good
    # Else return (False, ErrorMsg)
    # Error messages:
    #       - Device already issued?
    #       - Employee not in department?

    # return (False, "Device already issued")

    connection = database_connect()
    if(connection is None):
        return None
    cursor1 = connection.cursor()
    try:
        cursor1.execute("SELECT *\
        FROM employeedepartments\
        WHERE empid=%s", [employee_id])
    except:
        print("Error executing issue_device_to_employee1")
    result1 = cursor1.fetchall()
    cursor1.close()
    departs = [] # The employee may have many occupations
    for row in result1:
        departs.append(row[1])

    cursor = connection.cursor()
    try:
        cursor.execute("SELECT issuedto, department \
        FROM Device INNER JOIN ModelAllocations USING (manufacturer, modelnumber)\
        WHERE deviceid=%s",[device_id])
    except:
    # This happens if there is an error executing the query
        print("Error executing issue_device_to_employee2")
    results = cursor.fetchone()
    cursor.close() # IMPORTANT: close cursor
    if results[0] != None:
        connection.close() # IMPORTANT: close connection
        return (False, "This device already issued")
    if results[1] not in departs:
        connection.close() # IMPORTANT: close connection
        return (False, "The employee isn't in the department")

    cursor2 = connection.cursor()
    try:
        cursor2.execute("UPDATE Device\
                        SET issuedto=%s\
                        WHERE deviceid=%s", [employee_id, device_id])
    except:
        print("Error executing issue_device_to_employee3")
    connection.commit()
    cursor2.close()
    connection.close()
    return (True, None)


#####################################################
#   Query (f[vi])
#   Revoke Device Issued to User
#####################################################

def revoke_device_from_employee(employee_id: int, device_id: int):
    """
        Revoke the device from the employee.
        """
    connection = database_connect()
    if(connection is None):
        return None
    cursor1 = connection.cursor()
    try:
        cursor1.execute("SELECT issuedto\
                        FROM Device\
                        WHERE deviceid=%s", [device_id])
    except:
        print("Error executing revoke_device_from_employee1")
    result1 = cursor1.fetchone()
    cursor1.close()
    if len(result1) == 0:
        return(False, "Device has already been revoked.")
    if result1[0] != int(employee_id):
        return(False, "Employee is not assigned to this device.")

    cursor = connection.cursor()
    try:
        cursor.execute("UPDATE Device\
                        SET issuedto=NULL\
                        WHERE deviceid=%s", [device_id])
    except:
        # This happens if there is an error executing the query
        print("Error executing revoke_device_from_employee2")
    cursor.close()
    connection.commit()
    connection.close()

    return (True, None)

#####################################################
#   Get Employees in Department and Avaliable
#####################################################

def get_employee_status(department_name: str) -> list:

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT empid, name, department\
        FROM employeedepartments JOIN employee USING (empid)\
        ORDER BY empid")
    except:
    # This happens if there is an error executing the query
        print("Error executing get_employee_status")
    employee_list = cursor.fetchall()
    cursor.close()
    connection.close()

    output_list = []
    for employee in employee_list:
        if [employee[0],employee[1],False] in output_list:
            output_list.remove([employee[0],employee[1],False])
        elif [employee[0],employee[1],True] in output_list:
            continue

        if employee[2] == department_name:
            output_list.append([employee[0],employee[1],True])
        else:
            output_list.append([employee[0],employee[1],False])
            
    return output_list

#####################################################
#   Assign avaliable employees to the department
#####################################################

def assign(employee_id: int, department_name: str):

    connection = database_connect()
    if(connection is None):
        return None
    cursor1 = connection.cursor()
    try:
        cursor1.execute("SELECT *\
                        FROM EmployeeDepartments\
                        WHERE empid=%s and department=%s", (employee_id, department_name))
    except:
        print("Error executing assign")
    result1 = cursor1.fetchone()
    cursor1.close()
    if result1 != None:
        return(False, "Employee is already in the department.")

    cursor = connection.cursor()
    try:
        cursor.execute("INSERT INTO EmployeeDepartments (empid, department, fraction)\
                        VALUES (%s,%s,%s)", (employee_id, department_name, 10))
                        # The fraction here is setted to 10
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing assign2: {}".format(e))
    cursor.close()
    connection.commit()
    connection.close()

    return (True, None)

#####################################################
#   Dismiss employees in Department
#####################################################

def dismiss(employee_id: int, department_name: str):
    
    connection = database_connect()
    if(connection is None):
        return None
    cursor1 = connection.cursor()
    try:
        cursor1.execute("SELECT *\
                        FROM EmployeeDepartments\
                        WHERE empid=%s and department=%s", (employee_id, department_name))
    except:
        print("Error executing dismiss")
    result1 = cursor1.fetchone()
    cursor1.close()
    if result1 == None:
        return(False, "Employee is not in the department.")
    
    # When the employee to dismiss have devices coorelates to the department, revoke them
    cursor2 = connection.cursor()
    try:
        cursor2.execute("SELECT deviceid\
                        FROM Device JOIN modelallocations USING(manufacturer, modelnumber)\
                        WHERE issuedto=%s AND department=%s",[employee_id, department_name])
    except:
        print("Error executing dismiss1")
    result2 = cursor2.fetchall()
    cursor2.close()
    if result2 != None:
        cursor3 = connection.cursor()
        for device_id in result2:
            try:
                cursor3.execute("UPDATE Device\
                                SET issuedto=NULL\
                                WHERE deviceid=%s", [device_id[0]])
            except:
                # This happens if there is an error executing the query
                print("Error executing dismiss2")
        cursor3.close()

    cursor = connection.cursor()
    try:
        cursor.execute("DELETE FROM EmployeeDepartments\
                        WHERE empid=%s and department=%s", (employee_id, department_name))
    except:
        # This happens if there is an error executing the query
        print("Error executing dismiss3")
    cursor.close()
    connection.commit()
    connection.close()

    return (True, None)
