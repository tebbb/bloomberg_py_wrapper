"""A Bloomberg Wrapper to simplify data acquisition using the Bloomberg API
    Way of accessing the data:
        - Synchronous (blocks until data is fully returned)
        - Asynchronous (non-blocking)
        - Subscribe (Not Implemented) (for handling live data)
    Note that some requests are only usable in Asynchronous mode (eg: subscription type requests)

    Requirements:
        - pandas
        - blpapi
        - signal (for timeout errors)
        - datetime
        - threading (for Asynchronous & Subscription)

    Classes:
        - BloombergError
        - TimeoutError
        - _TimeoutWrapper (to be called by user function and raises TimeoutError)
        - Synchronous
        - Asynchronous
        - Subscribe (Not implemented)

    Notes on Data Limits (as of March 2018)
    There are three types of data limits:
        - Real Time Data limit: around 3500 subscriptions in parallel, with 1 subscription = 1 Ticker + 1 Field
        - Daily Hits limit: <500k hits, with 2 hits = (1 Ticker & 1 Field) x (1 initial request + 1 refresh)
        - Monthly Unique Identifiers limit: <5k unique identifiers, but it is not clear what an identifier is
    as fields and tickers carry different "costs"
"""

import pandas as _pd
import blpapi as _bloom
import signal as _signal
import datetime as _dt
import threading as _th


class _BloombergError(Exception):
    """An Error for Bloomberg related issues
        Example:
            - Can't create a session (ie connect to Bloomberg API)
            - Can't access a service
            - The request was malformed and cannot be processed
            - The user ran out of data allowance
    """
    pass


class _TimeoutError(Exception):
    """An Error for when a request times out, called by a _TimeoutWrapper object"""
    pass


class _TimeoutWrapper(object):
    """Uses the signal library to monitor for timeout of a request or a subscription"""
    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        # Start the alarm
        _signal.signal(_signal.SIGALRM, self.raise_timeout)
        _signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Disable the alarm
        _signal.alarm(0)

    def raise_timeout(self):
        # Raises the exception
        raise _TimeoutError('The request timed-out!')


class Synchronous(object):

    """A Class that enables the user to use blocking call functions.
        Once called, the function blocks the main program and gives back control on:
            - returning data
            - timeout
            - bloomberg error
        Recommended if the user only needs to do a single request at a time

        User Functions:
            - field_list_request
            - field_info_request
            - field_search_request
            - reference_data (normal and bulk)
            - historical_data
            - aim_position_data
            - aim_historical_data
            - aim_cash_service

        Support Functions:
            - _input_validation_type
            - _input_validation_list
            - _listening_bloomberg
            - _process_msg_error
            - _process_field_list_request
            - _process_field_info_request
            - _process_field_search_request
            - _process_reference_data
            - _process_historical_data
            - _process_aim_position_data
            - _process_aim_historical_data
            - _process_aim_cash_service
            - _timeout_wrapper
    """
    _host = 'localhost'
    _port = 8194

    def __init__(self):
        session_options = _bloom.SessionOptions()
        session_options.setServerHost(self._host)
        session_options.setServerPort(self._port)
        self._session = _bloom.Session(session_options)
        try:
            self._session.start()
        except Exception:
            raise _BloombergError('Failed to Start the Sessions!')

        # Generate the Bloomberg item name objects (pre-generated objects for speed))
        # Yes it is a mess
        self._bloom_name_objects = {'dateRange': _bloom.Name('dateRange'),
                                    'fromDate': _bloom.Name('fromDate'),
                                    'toDate': _bloom.Name('toDate'),
                                    'internalParameters': _bloom.Name('internalParameters'),
                                    'name': _bloom.Name('name'),
                                    'value': _bloom.Name('value'),
                                    'positionData': _bloom.Name('positionData'),
                                    'securityData': _bloom.Name('securityData'),
                                    'positionHistoryResponse': _bloom.Name('positionHistoryResponse'),
                                    'cashBalanceData': _bloom.Name('cashBalanceData'),
                                    'dataRow': _bloom.Name('dataRow'),
                                    'data': _bloom.Name('data'),
                                    'bookName': _bloom.Name('bookName'),
                                    'securityItem': _bloom.Name('securityItem'),
                                    'securityID': _bloom.Name('securityID'),
                                    'securityName': _bloom.Name('securityName'),
                                    'security': _bloom.Name('security'),
                                    'fieldData': _bloom.Name('fieldData'),
                                    'fieldId': _bloom.Name('fieldID'),
                                    'date': _bloom.Name('date'),
                                    'primeBroker': _bloom.Name('primeBroker'),
                                    'currency': _bloom.Name('currency'),
                                    'account': _bloom.Name('account'),
                                    'strategy': _bloom.Name('strategy'),
                                    'settleDateCash': _bloom.Name('settleDateCash'),
                                    'S': _bloom.Name('S'),
                                    'D': _bloom.Name('D'),
                                    'I': _bloom.Name('I'),
                                    'B': _bloom.Name('B'),
                                    'fieldExceptions': _bloom.Name('fieldExceptions'),
                                    'errorInfo': _bloom.Name('errorInfo'),
                                    'responseError': _bloom.Name('responseError'),
                                    'category': _bloom.Name('category'),
                                    'subcategory': _bloom.Name('subcategory'),
                                    'message': _bloom.Name('message'),
                                    'mnemonic': _bloom.Name('mnemonic'),
                                    'description': _bloom.Name('description'),
                                    'datatype': _bloom.Name('datatype'),
                                    'documentation': _bloom.Name('documentation'),
                                    'overrides': _bloom.Name('overrides'),
                                    'fieldInfo': _bloom.Name('fieldInfo'),
                                    'id': _bloom.Name('id'),
                                    'categoryName': _bloom.Name('categoryName'),
                                    'property': _bloom.Name('property'),
                                    'exclude': _bloom.Name('exclude'),
                                    'include': _bloom.Name('include'),
                                    'tickData': _bloom.Name('tickData'),
                                    'eidData': _bloom.Name('eidData'),
                                    'time': _bloom.Name('time'),
                                    'type': _bloom.Name('type'),
                                    'size': _bloom.Name('size')}

    def _send_request(self, request, output_data, timeout_seconds, call_function):
        """Sends the request and handles the timeout if there is any"""
        # Send the request
        cid = self._session.sendRequest(request)

        # Wait for Bloomberg to respond
        if timeout_seconds is not None:
            try:
                with _TimeoutWrapper(timeout_seconds):
                    return self._listen_bloomberg(cid=cid, output_data=output_data,
                                                  call_function=self._process_reference_data)
            except _TimeoutError:
                raise _TimeoutError('The reference_data request timed-out!')
        else:
            return self._listen_bloomberg(cid=cid, output_data=output_data, call_function=call_function)

    @staticmethod
    def _input_validation_type(variable, variable_name, allowed_types_list, can_be_none=False):
        """Checks if the data in variable corresponds to the allowed type
            Returns True if it is not None and False if it is None
        """
        var_type = type(variable)
        if can_be_none is False:
            if variable is None:
                raise ValueError('{v_name} cannot be of type: {type}, must be of type: {target}'.format(
                    v_name=variable_name, type=var_type, target=allowed_types_list))
            else:
                return False
        if variable is not None:
            if var_type not in allowed_types_list:
                raise ValueError('{v_name} cannot be of type: {type}, must be of type: {target}'.format(
                    v_name=variable_name, type=var_type, target=allowed_types_list))
        # Check if it is empty
        if var_type is str:
            if variable == '':
                raise ValueError('{v_name} must not be empty: {value}'.format(v_name=variable_name, value=variable))
        if var_type in [list, tuple]:
            if len(variable) == 0:
                raise ValueError('{v_name} must be of length > 0: {value}'.format(v_name=variable_name, value=variable))
            else:
                num_empty = 0
                for var in variable:
                    if var == '':
                        num_empty += 1
                if num_empty == len(variable):
                    raise ValueError('{v_name} must not be empty: {value}'.format(v_name=variable_name, value=variable))
        if var_type is dict:
            if len(variable) == 0:
                raise ValueError('{v_name} must be of length > 0: {value}'.format(v_name=variable_name, value=variable))
            else:
                num_empty = 0
                for key, item in variable:
                    if item == '':
                        num_empty += 1
                if num_empty == len(variable):
                    raise ValueError('{v_name} must not be empty: {value}'.format(v_name=variable_name, value=variable))
        # All good
        if variable is None:
            return False
        else:
            return True

    @staticmethod
    def _input_validation_list(variable, variable_name, possible_values, can_be_none=False):
        """Returns True if the type of the data is allowed within a list of possibles"""
        var_type = type(variable)
        if can_be_none is False:
            if variable is None:
                raise ValueError('{v_name} cannot be of type: {type}'.format(v_name=variable_name, type=var_type))
            else:
                return False
        else:
            if variable not in possible_values:
                raise ValueError('{v_name} of value: {value}; must be part of the set: {values}'.format(
                    v_name=variable_name, value=variable, values=possible_values))
        # All good
        return True

    def field_list_request(self, field_type, return_field_documentation=True, timeout_seconds=None):
        """Returns a list of fields matching the field_type
            Inputs:
                - field_type: must be either: ALL, STATIC, REAL_TIME
                - return_field_documentation: True if you want the documentation
        """
        # Internal constants and memory
        service_address = '//blp/apiflds'
        request_type = 'FieldListRequest'

        # Input validation
        self._input_validation_list(field_type, 'field_type', ('All', 'Static', 'RealTime'), False)
        self._input_validation_type(return_field_documentation, 'return_field_documentation', (bool,), False)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))

        # Opening the Service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to field list request service!')
        field_list_request_service = self._session.getService(service_address)
        request = field_list_request_service.createRequest(request_type)

        # Populate the data
        request.set('fieldType', field_type)
        request.set('returnFieldDocumentation', return_field_documentation)

        # Pre-allocate the output data
        output_data = _pd.DataFrame(columns=['fieldId', 'Mnemonic', 'Description', 'DataType',
                                             'Documentation', 'Category', 'Overrides'])
        output_data.set_index('fieldId', inplace=True)

        # Sending the request
        return self._send_request(request, output_data, timeout_seconds, self._process_field_list_request)

    def field_info_request(self, field_ids, return_field_documentation=True, include_field_overridable=False,
                           timeout_seconds=None):
        """Returns a description for each valid field_id requested
            Inputs:
                - field_id: must be a string, list, or tuple
                - return_field_documentation: True if you want the documentation
        """
        # Internal constants and memory
        field_ids_iterable = False
        service_address = '//blp/apiflds'
        request_type = 'FieldInfoRequest'

        # Input Validation
        self._input_validation_type(field_ids, 'field_ids', (str, tuple, list), False)
        if isinstance(field_ids, (tuple, list)):
            field_ids_iterable = True
        self._input_validation_type(return_field_documentation, 'return_field_documentation', (bool,), False)
        self._input_validation_type(include_field_overridable, 'include_field_overridable', (bool,), False)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))

        # Opening the Service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to field list request service!')
        field_list_request_service = self._session.getService(service_address)
        request = field_list_request_service.createRequest(request_type)

        # Populate the data
        if field_ids_iterable is True:
            for field_id in field_ids:
                request.append('id', field_id)
        else:
            request.append('id', field_ids)
            field_ids = [field_ids]
        request.set('returnFieldDocumentation', return_field_documentation)
        if include_field_overridable is True:
            request.append('properties', 'fieldoverridable')

        # Pre-allocate the output data
        if include_field_overridable is True:
            output_data = _pd.DataFrame(index=field_ids, columns=['fieldId', 'Mnemonic', 'Description', 'DataType',
                                                                  'Documentation', 'Overrides', 'Overridable'])
        else:
            output_data = _pd.DataFrame(index=field_ids, columns=['fieldId', 'Mnemonic', 'Description', 'DataType',
                                                                  'Documentation', 'Overrides'])

        # Sending the request
        return self._send_request(request, output_data, timeout_seconds, self._process_field_info_request)

    def field_search_request(self, search_spec, conditions, return_field_documentation=True, timeout_seconds=None):
        """Enables the user to perform a filtered search using key words in search_spec
            returns a list of matching fields and their descriptions
            Inputs:
                - search_speac: what is being searched through mnemonics, descriptions, and definitions
                - include_field_overridable: True if you want to know if the field is overridable
                - return_field_documentation: True if you want the documentation
                - conditions: a dictionary structured as:
                    {'include': {'product_type': ['A', 'B']}, exclude: {}}
                    with possible entries as:
                    - product_typee: All, Govt, Corp, Mtge, M-Mkt, Muni, Pfd, Equity, Cmdty, Index, Curncy
                    - field_type: All, Static, RealTime
                    - bps_requirements: (if bloomberg professional licence is required for the field), All, BPS, NoBPS
                    - category: New Fields, Analysis, Corporate, Actions, Custom Fields, Descriptive, Earning,
                                Estimates, Fundamentals, Market Activity, Metadata, Ratings, Trading, Systems
        """
        # Internal Constants and Memory
        service_address = '//blp/apiflds'
        request_type = 'FieldSearchRequest'

        # Input Validation
        self._input_validation_type(search_spec, 'search_spec', (str,), False)
        self._input_validation_type(conditions, 'conditions', (dict,), False)
        self._input_validation_type(return_field_documentation, 'return_field_documentation', (bool,), False)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))

        # Open the service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to field list request service!')
        field_list_request_service = self._session.getService(service_address)
        request = field_list_request_service.createRequest(request_type)

        # Populate the Data and validate the "conditions" field
        request.set('searchSpec', search_spec)
        if 'exclude' in conditions.keys():
            exclude = request.getElement(self._bloom_name_objects['exclude'])
            if 'product_type' in conditions['exclude']:
                product_type = conditions['exclude']['product_type']
                self._input_validation_type(product_type, 'product_type', (str, list, tuple), False)
                if isinstance(product_type, str):
                    self._input_validation_list(product_type, 'product_type', ('All', 'Govt', 'Corp', 'Mtge', 'M-Mkt',
                                                                               'Muni', 'Pfd', 'Equity', 'Cmdty',
                                                                               'Index', 'Curncy'), False)
                    exclude.setElement('product_type', product_type)
                else:
                    product_type_exclude = exclude.getElement('product_type')
                    for item in product_type:
                        self._input_validation_list(item, 'product_type', ('All', 'Govt', 'Corp', 'Mtge',
                                                                           'M-Mkt', 'Muni', 'Pfd', 'Equity',
                                                                           'Cmdty', 'Index', 'Curncy'), False)
                        product_type_exclude.appendValue(item)

            elif 'field_type' in conditions['exclude']:
                field_type = conditions['exclude']['field_type']
                self._input_validation_list(field_type, 'field_type', (str, list, tuple), False)
                if isinstance(field_type, str):
                    self._input_validation_list(field_type, 'field_type', ('All', 'Static', 'RealTime'), False)
                    exclude.setElement('field_type', field_type)
                else:
                    field_type_exclude = exclude.getElement('field_type')
                    for item in field_type:
                        self._input_validation_list(item, 'field_type', ('All', 'Static', 'RealTime'), False)
                        field_type_exclude.appendValue(item)

            elif 'bps_requirements' in conditions['exclude']:
                bps_requirements = conditions['exclude']['bps_requirements']
                self._input_validation_type(bps_requirements, 'bps_requirements', (str, list, tuple), False)
                if isinstance(bps_requirements, str):
                    self._input_validation_list(bps_requirements, 'bps_requirements', ('All', 'BPS', 'NoBPS'), False)
                    exclude.setElement('bps_requirements', bps_requirements)
                else:
                    bps_requirements_exclude = exclude.getElement('bps_requirements')
                    for item in bps_requirements:
                        self._input_validation_list(item, 'bps_requirements', ('All', 'BPS', 'NoBPS'), False)
                        bps_requirements_exclude.appendValue(item)

            elif 'category' in conditions['exclude']:
                category = conditions['exclude']['category']
                self._input_validation_type(category, 'category', (str, list, tuple), False)
                if isinstance(category, str):
                    self._input_validation_list(category, 'category', ('New Fields', 'Analysis', 'Corporate',
                                                                       'Actions', 'Custom Fields', 'Descriptive',
                                                                       'Earning', 'Fundamentals', 'Market Activity',
                                                                       'Metadata', 'Ratings', 'Trading', 'System'),
                                                False)
                    exclude.setElement('category', category)
                else:
                    category_exclude = exclude.getElement('category')
                    for item in category:
                        self._input_validation_list(item, 'category', ('New Fields', 'Analysis', 'Corporate',
                                                                       'Actions', 'Custom Fields', 'Descriptive',
                                                                       'Earning', 'Fundamentals', 'Market Activity',
                                                                       'Metadata', 'Ratings', 'Trading', 'System'),
                                                    False)
                        category_exclude.appendValue(item)

        elif 'include' in conditions.keys():
            include = request.getElement(self._bloom_name_objects['include'])
            if 'product_type' in conditions['include']:
                product_type = conditions['exclude']['product_type']
                self._input_validation_type(product_type, 'product_type', (str, list, tuple), False)
                if isinstance(product_type, str):
                    self._input_validation_list(product_type, 'product_type', ('All', 'Govt', 'Corp', 'Mtge', 'M-Mkt',
                                                                               'Muni', 'Pfd', 'Equity', 'Cmdty',
                                                                               'Index', 'Curncy'), False)
                    include.setElement('product_type', product_type)
                else:
                    product_type_include = include.getElement('product_type')
                    for item in product_type:
                        self._input_validation_list(item, 'product_type', ('All', 'Govt', 'Corp', 'Mtge',
                                                                           'M-Mkt', 'Muni', 'Pfd', 'Equity',
                                                                           'Cmdty', 'Index', 'Curncy'), False)
                        product_type_include.appendValue(item)

            elif 'field_type' in conditions['include']:
                field_type = conditions['exclude']['field_type']
                self._input_validation_list(field_type, 'field_type', (str, list, tuple), False)
                if isinstance(field_type, str):
                    self._input_validation_list(field_type, 'field_type', ('All', 'Static', 'RealTime'), False)
                    include.setElement('field_type', field_type)
                else:
                    field_type_include = include.getElement('field_type')
                    for item in field_type:
                        self._input_validation_list(item, 'field_type', ('All', 'Static', 'RealTime'), False)
                        field_type_include.appendValue(item)

            elif 'bps_requirements' in conditions['include']:
                bps_requirements = conditions['exclude']['bps_requirements']
                self._input_validation_type(bps_requirements, 'bps_requirements', (str, list, tuple), False)
                if isinstance(bps_requirements, str):
                    self._input_validation_list(bps_requirements, 'bps_requirements', ('All', 'BPS', 'NoBPS'), False)
                    include.setElement('bps_requirements', bps_requirements)
                else:
                    bps_requirements_include = include.getElement('bps_requirements')
                    for item in bps_requirements:
                        self._input_validation_list(item, 'bps_requirements', ('All', 'BPS', 'NoBPS'), False)
                        bps_requirements_include.appendValue(item)

            elif 'category' in conditions['include']:
                category = conditions['exclude']['category']
                self._input_validation_type(category, 'category', (str, list, tuple), False)
                if isinstance(category, str):
                    self._input_validation_list(category, 'category', ('New Fields', 'Analysis', 'Corporate',
                                                                       'Actions', 'Custom Fields', 'Descriptive',
                                                                       'Earning', 'Fundamentals', 'Market Activity',
                                                                       'Metadata', 'Ratings', 'Trading', 'System'),
                                                False)
                    include.setElement('category', category)
                else:
                    category_include = include.getElement('category')
                    for item in category:
                        self._input_validation_list(item, 'category', ('New Fields', 'Analysis', 'Corporate',
                                                                       'Actions', 'Custom Fields', 'Descriptive',
                                                                       'Earning', 'Fundamentals', 'Market Activity',
                                                                       'Metadata', 'Ratings', 'Trading', 'System'),
                                                    False)
                        category_include.appendValue(item)

        request.set('returnFieldDocumentation', return_field_documentation)

        # Pre-allocate the output data
        output_data = _pd.DataFrame(columns=['fieldId', 'Mnemonic', 'Description', 'DataType',
                                             'Documentation', 'Category', 'Overrides'])
        output_data.set_index('fieldId', inplace=True)

        # Sending the request
        return self._send_request(request, output_data, timeout_seconds, self._process_field_search_request)

    def reference_data(self, tickers, fields, overrides=None, timeout_seconds=None):
        """Returns reference data in a dataframe, if there is bulk data it is as a dictionary inside the dataframe
            Inputs:
                - tickers: string, list, tuple
                - fields: string, list, tuple
                - overrides: dictionary, None
                - timeout_seconds: int, float, None
        """
        # Internal constants and memory
        tickers_iterable = False
        fields_iterable = False
        overrides_exist = False
        service_address = '//blp/refdata'
        request_type = 'ReferenceDataRequest'

        # Input Validation
        self._input_validation_type(tickers, 'tickers', (str, tuple, list), False)
        if isinstance(tickers, (tuple, list)):
            tickers_iterable = True
        self._input_validation_type(fields, 'fields', (str, tuple, list), False)
        if isinstance(fields, (tuple, list)):
            fields_iterable = True
        overrides_exist = self._input_validation_type(overrides, 'overrides', (dict,), True)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))

        # Opening the Service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to reference data service!')
        reference_data_service = self._session.getService(service_address)
        request = reference_data_service.createRequest(request_type)

        # Populate the Data
        num_tickers = 0
        num_fields = 0
        selected_tickers = []
        selected_fields = []
        if tickers_iterable is True:
            for tick in tickers:
                if type(tick) is str and tick != '':
                    num_tickers += 1
                    request.append('securities', tick)
                    selected_tickers.append(tick)
        else:
            num_tickers += 1
            request.append('securities', tickers)
            selected_tickers.append(tickers)
        if num_tickers == 0:
            raise ValueError('tickers must have at least one non empty string!')
        if fields_iterable is True:
            for fld in fields:
                if type(fld) is str and fld != '':
                    num_fields += 1
                    request.append('fields', fld)
                    selected_fields.append(fld)
        else:
            num_fields += 1
            request.append('fields', fields)
            selected_fields.append(fields)
        if num_fields == 0:
            raise ValueError('fields must have at least one non empty string!')
        if overrides_exist is True:
            override_objects = request.getElement('overrides')
            for key, value in overrides.iteritems():
                ovd = override_objects.appendElement()
                ovd.setElement('fieldId', key)
                ovd.setElement('value', value)

        # Pre-allocate the output data
        output_data = _pd.DataFrame(index=selected_tickers, columns=selected_fields)

        # Sending the Request
        return self._send_request(request, output_data, timeout_seconds, self._process_reference_data)

    def historical_data(self, tickers, fields, start_date, end_date, overrides=None, timeout_seconds=None,
                        align_dates=False, periodicity_adjustment=None, periodicity_selection=None, currency=None,
                        override_option=None, pricing_option=None, non_trading_day_fill_option=None,
                        non_trading_day_fill_method=None, max_data_points=None, return_eids=None,
                        return_relative_date=None, adjustment_normal=None, adjustment_abnormal=None,
                        adjustment_split=None, adjustment_follow_dpdf=None, calendar_code_override=None):
        """Returns historical data in a multiindex dataframe with two configurations based on the option align_dates
            align_dates is True:
                index is the dates array (all the dates returned) if some tickers do not have date for a date, the
                previous value is used (or the setting used in non_trading_day_fill_option)
                the top column index is the ticker, with each ticker column having the fields as sub-columns
            align_dates is False:
                index is a non-significant number
                the top column index is the ticker, with each ticker column having the fields as sub-columns,
                however in the fields a 'date' is added
            Inputs:
                - tickers: string, list, tuple
                - fields: string, list, tuple
                - start_date: _dt.datetime, _dt.date
                - end_date: _dt.datetime, _dt.date
                - overrides: dict, None
                - timeout_seconds: int, float, None
                - align_dates: bool (True means that the data for each ticker will share a common dates array)
                - periodicity_adjustment: 'ACTUAL', 'CALENDAR', 'FISCAL'
                - periodicity_selection: 'DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'SEMI_ANNUALLY', 'YEARLY'
                - currency: some currency code
                - override_option: 'OVERRIDE_OPTION_CLOSE', 'OVERRIDE_OPTION_GPA'
                - pricing_option: 'PRICING_OPTION_PRICE', 'PRICING_OPTION_YIELD'
                - non_trading_day_fill_option: 'NON_TRADING_WEEKDAYS', 'ALL_CALENDAR_DAYS', 'ACTIVE_DAYS_ONLY'
                - non_trading_day_fill_method: 'PREVIOUS_VALUE', 'NIL_VALUE'
                - max_data_points: some integer number
                - return_eids: 'TRUE', 'FALSE'
                - return_relative_date: 'TRUE', 'FALSE'
                - adjustment_normal: 'TRUE', 'FALSE'
                - adjustment_abnormal: 'TRUE', 'FALSE'
                - adjustment_split: 'TRUE', 'FALSE'
                - adjustment_follow_dpdf: 'TRUE', 'FALSE'
                - calendar_code_override: some calendar code
        """

        # Internal Memory
        tickers_iterable = False
        fields_iterable = False
        overrides_exist = False
        service_address = '//blp/refdata'
        request_type = 'HistoricalDataRequest'
        bloomberg_overrides = {}

        # Input Validation
        self._input_validation_type(tickers, 'tickers', (str, tuple, list), False)
        if isinstance(tickers, (tuple, list)):
            tickers_iterable = True
        self._input_validation_type(fields, 'fields', (str, tuple, list), False)
        if isinstance(fields, (tuple, list)):
            fields_iterable = True
        self._input_validation_type(start_date, 'start_date', (_dt.date, _dt.datetime), False)
        self._input_validation_type(end_date, 'end_date', (_dt.date, _dt.datetime), False)
        if start_date > end_date:  # Might want to test difference in date type only, not minutes
            raise ValueError('start_date: {start} must be the same or before end_date: {end}'.format(start=start_date,
                                                                                                     end=end_date))
        overrides_exist = self._input_validation_type(overrides, 'overrides', (dict,), True)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))
        periodicity_selection_not_none = self._input_validation_list(periodicity_selection, 'periodicity_selection',
                                                                     ('ACTUAL', 'CALENDAR', 'FISCAL'), True)
        if periodicity_selection_not_none is True:
            bloomberg_overrides['periodicitySelection'] = periodicity_selection
        periodicity_adjustment_not_none = self._input_validation_list(periodicity_adjustment, 'periodicity_adjusment',
                                                                      ('DAILY', 'WEEKLY', 'QUARTERLY', 'SEMI_ANNUALLY',
                                                                       'YEARLY'), True)
        if periodicity_adjustment_not_none is True:
            bloomberg_overrides['periodicityAdjustment'] = periodicity_adjustment
        currency_not_none = self._input_validation_list(currency, 'currency', (str,), True)
        if currency_not_none is True:
            bloomberg_overrides['currency'] = currency
        override_option_not_none = self._input_validation_list(override_option, 'override_option',
                                                               ('OVERRIDE_OPTION_CLOSE', 'OVERRIDE_OPTION_GPA'), True)
        if override_option_not_none is True:
            bloomberg_overrides['overrideOption'] = override_option
        pricing_option_not_none = self._input_validation_list(pricing_option, 'pricing_option',
                                                              ('PRICING_OPTION_PRICE', 'PRICING_OPTION_YIELD'), True)
        if pricing_option_not_none is True:
            bloomberg_overrides['pricingOption'] = pricing_option
        non_trading_day_fill_option_not_none = self._input_validation_list(non_trading_day_fill_option,
                                                                           'non_trading_day_fill_option',
                                                                           ('NON_TRADING_WEEKDAYS', 'ALL_CALENDAR_DAYS',
                                                                            'ACTIVE_DAYS_ONLY'), True)
        if non_trading_day_fill_option_not_none is True:
            bloomberg_overrides['nonTradingDayFillOption'] = non_trading_day_fill_option
        non_trading_day_fill_method_not_none = self._input_validation_list(non_trading_day_fill_method,
                                                                           'non_trading_day_fill_method',
                                                                           ('PREVIOUS_VALUE', 'NIL_VALUE'), True)
        if non_trading_day_fill_method_not_none is True:
            bloomberg_overrides['nonTradingDayFillMethod'] = non_trading_day_fill_method
        max_data_points_not_none = self._input_validation_type(max_data_points, 'max_data_points', (int, float, long),
                                                               True)
        if max_data_points_not_none is True:
            bloomberg_overrides['maxDataPoints'] = max_data_points
        return_eids_not_none = self._input_validation_type(return_eids, 'return_eids', (bool,), True)
        if return_eids_not_none is True:
            bloomberg_overrides['returnEids'] = return_eids
        return_relative_date_not_none = self._input_validation_type(return_relative_date, 'return_relative_date',
                                                                    (bool,), True)
        if return_relative_date_not_none is True:
            bloomberg_overrides['returnRelativeDate'] = return_relative_date
        adjustment_normal_not_none = self._input_validation_type(adjustment_normal, 'adjustment_normal', (bool,), True)
        if adjustment_normal_not_none is True:
            bloomberg_overrides['adjustmentNormal'] = adjustment_normal
        adjustment_abnormal_not_none = self._input_validation_type(adjustment_abnormal, 'adjustment_abnormal',
                                                                   (bool,), True)
        if adjustment_abnormal_not_none is True:
            bloomberg_overrides['adjustmentAbnormal'] = adjustment_abnormal
        adjustment_split_not_none = self._input_validation_type(adjustment_split, 'adjustment_split', (bool,), True)
        if adjustment_split_not_none is True:
            bloomberg_overrides['adjustmentSplit'] = adjustment_split
        adjustment_follow_dpdf_not_none = self._input_validation_type(adjustment_follow_dpdf, 'adjustment_follow_dpdf',
                                                                      (bool,), True)
        if adjustment_follow_dpdf_not_none is True:
            bloomberg_overrides['adjustmentFollowDpdf'] = adjustment_follow_dpdf
        calendar_code_override_not_none = self._input_validation_type(calendar_code_override, 'calendar_code_override',
                                                                      (str,), True)
        if calendar_code_override_not_none is True:
            bloomberg_overrides['calendarCodeOverride'] = calendar_code_override

        # Open the service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to historical data service!')
        historical_data_service = self._session.getService(service_address)
        request = historical_data_service.createRequest(request_type)

        # Populate the data
        num_tickers = 0
        num_fields = 0
        selected_tickers = []
        selected_fields = []
        if tickers_iterable is True:
            for tick in tickers:
                if type(tick) is str and tick != '':
                    num_tickers += 1
                    request.append('securities', tick)
                    selected_tickers.append(tick)
        else:
            num_tickers += 1
            request.append('securities', tickers)
            selected_tickers.append(tickers)
        if num_tickers == 0:
            raise ValueError('tickers must have at least one non empty string!')
        if fields_iterable is True:
            for fld in fields:
                if type(fld) is str and fld != '':
                    num_fields += 1
                    request.append('fields', fld)
                    selected_fields.append(fld)
        else:
            num_fields += 1
            request.append('fields', fields)
            selected_fields.append(fields)
        if num_fields == 0:
            raise ValueError('fields must have at least one non empty string!')
        request.set('startDate', '{}{:02d}{:02d}'.format(start_date.year, start_date.month, start_date.day))
        request.set('endDate', '{}{:02d}{:02d}'.format(end_date.year, end_date.month, end_date.day))
        if overrides_exist is True:
            override_objects = request.getElement('overrides')
            for key, value in overrides.iteritems():
                ovd = override_objects.appendElement()
                ovd.setElement('fieldId', key)
                ovd.setElement('value', value)
        if len(bloomberg_overrides) > 0:
            for key, value in bloomberg_overrides.iteritems():
                request.set(key, value)

        # Pre-allocate the output data
        if align_dates is True:
            # Index has the dates
            output_data = _pd.DataFrame(columns=_pd.MultiIndex.from_product([selected_tickers, selected_fields],
                                                                            names=('tickers', 'fields')))
        else:
            # Index is random numbers and we add a 'date' entry in the fields
            fields.append('date')
            output_data = _pd.DataFrame(columns=_pd.MultiIndex.from_product([selected_tickers, selected_fields],
                                                                            names=('tickers', 'fields')))

        # Sending the Request
        return self._send_request(request, output_data, timeout_seconds, self._process_historical_data)

    def intraday_tick_data(self, ticker, start_date, end_date, event_types, timeout_seconds=None,
                           include_condition_codes=False, include_non_plottable_events=False,
                           include_exchange_codes=False, return_eids=False, include_broker_codes=False,
                           include_rps_codes=False, include_bic_mic_code=False, forced_delay=False,
                           include_spread_price=False, include_yield=False, include_action_codes=False,
                           include_indicator_codes=False, include_trade_time=True, include_upfront_price=False,
                           include_eq_ref_price=False, adjustment_normal=False, adjustment_abnormal=False,
                           adjustment_split=False, adjustment_follow_dpdf=False, include_client_specific_fields=False,
                           include_trade_id=False):
        """Returns the intra-day tick data for the selected securities within the last 140 business days
            The data is returned as a pandas DataFrame object
            Inputs:
                - ticker: str (only one security)
                - start_date: _dt.date or _dt.datetime (UTC time!)
                - end_date: _dt.date or _dt.datetime   (UTC time!)
                - event_types: str, list, tuple
                - include_condition_codes: True, False
                - include_non_plottable_events: True, False
                - include_exchange_codes: True, False
                - return_eids: True, False
                - include_broker_codes: True, False
                - include_rps_codes: True, False
                - include_bic_codes: True, False
                - include_action_codes: True, False
                - include_indicator_codes: True, False
                - include_trade_time: True, False
                - include_upfront_price: True, False
                - include_eq_ref_price: True, False
                - adjustment_normal: True, False
                - adjustment_split: True, False
                - adjustment_follow_dpdf: True, False
                - include_client_specific_fields: True, False
                - include_trade_id: True, False
            """

        # Internal Memory
        event_types_iterable = False
        service_address = '//blp/refdata'
        request_type = 'IntradayTickRequest'

        # Input Validation
        self._input_validation_type(ticker, 'ticker', (str,), False)
        self._input_validation_type(start_date, 'start_date', (_dt.date, _dt.datetime), False)
        if isinstance(start_date, _dt.date):
            start_date = _dt.datetime(year=start_date.year, month=start_date.month, day=start_date.day,
                                      hour=0, minute=0, second=1)
        self._input_validation_type(end_date, 'end_date', (_dt.date, _dt.datetime), False)
        if isinstance(end_date, _dt.date):
            end_date = _dt.datetime(year=end_date.year, month=end_date.month, day=end_date.day,
                                    hour=0, minute=0, second=1)
        if start_date > end_date:
            raise ValueError('start_date: {start} must be the same or before end_date: {end}'.format(start=start_date,
                                                                                                     end=end_date))
        if isinstance(event_types, (list, tuple)):
            for event_type in event_types:
                self._input_validation_list(event_type, 'event_types', ('TRADE', 'BID', 'ASK', 'BID_BEST', 'ASK_BEST',
                                                                        'BID_YIELD', 'ASK_YIELD', 'MID_PRICE',
                                                                        'AT_TRADE', 'BEST_BID', 'BEST_ASK', 'SETTLE'),
                                            False)
            event_types_iterable = True
        else:
            self._input_validation_list(event_types, 'event_types', ('TRADE', 'BID', 'ASK', 'BID_BEST', 'ASK_BEST',
                                                                     'BID_YIELD', 'ASK_YIELD', 'MID_PRICE', 'AT_TRADE',
                                                                     'BEST_BID', 'BEST_ASK', 'SETTLE'), False)
        if isinstance(timeout_seconds, (int, float,)) and timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError('You must set the timeout at more than 0: {}'.format(timeout_seconds))
        self._input_validation_type(include_condition_codes, 'include_condition_codes', (bool,), False)
        self._input_validation_type(include_non_plottable_events, 'include_non_plottable_events', (bool,), False)
        self._input_validation_type(include_exchange_codes, 'include_exchange_codes', (bool,), False)
        self._input_validation_type(return_eids, 'return_eids', (bool,), False)
        self._input_validation_type(include_broker_codes, 'include_broker_codes', (bool,), False)
        self._input_validation_type(include_rps_codes, 'include_rps_codes', (bool,), False)
        self._input_validation_type(include_bic_mic_code, 'include_bic_codes', (bool,), False)
        self._input_validation_type(forced_delay, 'forced_delay', (bool,), False)
        self._input_validation_type(include_spread_price, 'include_spread_price', (bool,), False)
        self._input_validation_type(include_yield, 'include_yield', (bool,), False)
        self._input_validation_type(include_action_codes, 'include_action_codes', (bool,), False)
        self._input_validation_type(include_indicator_codes, 'include_indicator_codes', (bool,), False)
        self._input_validation_type(include_trade_time, 'include_trade_time', (bool,), False)
        self._input_validation_type(include_upfront_price, 'include_upfront_price', (bool,), False)
        self._input_validation_type(include_eq_ref_price, 'include_eq_ref_price', (bool,), False)
        self._input_validation_type(adjustment_normal, 'adjustment_normal', (bool,), False)
        self._input_validation_type(adjustment_abnormal, 'adjustment_abnormal', (bool,), False)
        self._input_validation_type(adjustment_split, 'adjustment_split', (bool,), False)
        self._input_validation_type(adjustment_follow_dpdf, 'adjustment_follow_dpdf', (bool,), False)
        self._input_validation_type(include_client_specific_fields, 'include_client_specific_fields', (bool,), False)
        self._input_validation_type(include_trade_id, 'include_trade_id', (bool,), False)

        # Open the Service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to historical data service!')
        intraday_tick_data_service = self._session.getService(service_address)
        request = intraday_tick_data_service.createRequest(request_type)

        # Populate the Data
        request.set('security', ticker)
        request.set('startDateTime', start_date)
        request.set('endDateTime', end_date)
        if event_types_iterable is True:
            for event_type in event_types:
                request.append('eventTypes', event_type)
        else:
            request.set('eventType', event_types)
        request.set('includeConditionCodes', include_condition_codes)
        request.set('includeNonPlottableEvents', include_non_plottable_events)
        request.set('includeExchangeCodes', include_exchange_codes)
        request.set('returnEids', return_eids)
        request.set('includeBrokerCodes', include_broker_codes)
        request.set('includeRpsCodes', include_rps_codes)
        request.set('includeBicMicCodes', include_bic_mic_code)
        request.set('forcedDelay', forced_delay)
        request.set('includeSpreadPrice', include_spread_price)
        request.set('includeYield', include_yield)
        request.set('includeActionCodes', include_action_codes)
        request.set('includeIndicatorCodes', include_indicator_codes)
        request.set('includeTradeTime', include_trade_time)
        request.set('includeUpfrontPrice', include_upfront_price)
        request.set('includeEqRefPrice', include_eq_ref_price)
        request.set('adjustmentNormal', adjustment_normal)
        request.set('adjustmentAbnormal', adjustment_abnormal)
        request.set('adjustmentSplit', adjustment_split)
        request.set('adjustmentFollowDPDF', adjustment_follow_dpdf)
        request.set('includeClientSpecificFields', include_client_specific_fields)
        request.set('includeTradeId', include_trade_id)

        # Pre-allocate the output data
        output_data = _pd.DataFrame(columns=['time', 'type', 'value', 'size'])

        # Sending the Request
        return self._send_request(request, output_data, timeout_seconds, self._process_intraday_tick_data)

    def aim_position_data(self, account, account_type, fields, include_cash=True, timeout_seconds=None):
        """Returns position data in a dataframe
            Notes: Requires a Bloomberg Subscription to AIM services
            Inputs:
                - account: the account for which to get the positions
                - account_type: Account or Group
                - fields: str, list, tuple
                - include_cash: bool, True if the cash positions should be added to the result
                - timeout_seconds: float, int the time before the request times-out and is abandoned
            Default Fields:
                - ACCOUNT: if there a multiple accounts requested (through a group) the source account is provided
                - TICKER: the ticker of the security
                - NAME: the name of the security
        """

        # Internal Memory
        fields_iterable = False
        service_address = '//blp/tseapi'
        request_type = 'EapiRequestPosition'

        # Input Validation
        self._input_validation_type(account, 'account', (str,), False)
        self._input_validation_list(account_type, 'account_type', ('ACCOUNT', 'GROUP'), False)
        fields_not_none = self._input_validation_list(fields, 'fields', (str, tuple, list), False)
        if isinstance(fields, (tuple, list)):
            fields_iterable = True
        self._input_validation_type(include_cash, 'include_cash', (bool,), False)

        # Open the service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to aim positions data service!')
        position_service = self._session.getService(service_address)
        request = position_service.createRequest(request_type)

        # Populate the data
        selected_fields = ['ACCOUNT', 'TICKER', 'NAME']
        request.set('entityType', account_type)
        request.set('entityName', account)
        if fields_iterable is True:
            for fld in fields:
                if type(fld) is str and fld != '':
                    request.append('fields', fld)
                    selected_fields.append(fld)
        else:
            if fields_not_none is True:
                request.append('fields', fields)
                selected_fields.append(fields)
        if include_cash is True:
            internal_parameters = request.getElement(self._bloom_name_objects['internalParameters'])
            cash_parameter = internal_parameters.appendElement()
            cash_parameter.getElement(self._bloom_name_objects['name']).setValue('IncludeCash')
            cash_parameter.getElement(self._bloom_name_objects['value']).setValue('Y')

        # Pre-allocate output data
        output_data = _pd.DataFrame(columns=selected_fields)

        # Send the Request
        return self._send_request(request, output_data, timeout_seconds, self._process_aim_position_data)

    def aim_historical_position_data(self, account, account_type, fields, start_date, end_date, include_cash=True,
                                     timeout_seconds=None):
        """Returns position data in a dataframe
            Notes: Requires a Bloomberg Subscription to AIM services
            Inputs:
                - account: the account for which to get the positions
                - account_type: Account or Group
                - fields: str, list, tuple
                - start_date: dt.datetime, dt.date
                - end_date: dt.datetime, dt.date
                - include_cash: bool, True if the cash positions should be added to the result
                - timeout_seconds: float, int the time before the request times-out and is abandoned
            Default Fields:
                - DATE: returns the date from which the portfolio is extracted
                - ACCOUNT: if there a multiple accounts requested (through a group) the source account is provided
                - TICKER: the ticker of the security
                - NAME: the name of the security
            How far back:
                There is a limit on how far back one can request the data (90 days ?)
            What time is the data saved:
                The account manager of the firm should be the one deciding at what time the historical data is saved
        """

        # Internal Memory
        fields_iterable = False
        service_address = '//blp/tsadf'
        request_type = 'RequestPositionHistory'

        # Input Validation
        self._input_validation_list(account, 'account', (str,), False)
        self._input_validation_list(account_type, 'account_type', ('ACCOUNT', 'GROUP'), False)
        fields_not_none = self._input_validation_type(fields, 'fields', (str, tuple, list), True)
        if isinstance(fields, (tuple, list,)):
            fields_iterable = True
        self._input_validation_type(start_date, 'start_date', (_dt.date, _dt.datetime), False)
        self._input_validation_type(end_date, 'end_date', (_dt.date, _dt.datetime), False)
        if start_date > end_date:  # Here check only for the date
            raise ValueError('start_date: {start} must be the same or before end_date: {end}'.format(start=start_date,
                                                                                                     end=end_date))
        self._input_validation_type(include_cash, 'include_cash', (bool,), False)

        # Opening the service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to the historical aim positions data service!')
        historical_position_service = self._session.getService(service_address)
        request = historical_position_service.createRequest(request_type)

        # Populate the data
        selected_fields = ['DATE', 'ACCOUNT', 'TICKER', 'NAME']
        request.set('entityType', account_type)
        request.set('entityName', account)
        if fields_iterable is True:
            for fld in fields:
                if type(fld) is str and fld != '':
                    request.append('fields', fld)
                    selected_fields.append(fld)
        else:
            if fields_not_none is True:
                request.append('fields', fields)
                selected_fields.append(fields)
        date_range = request.getElement(self._bloom_name_objects['dateRange'])
        date_range.getElement(self._bloom_name_objects['fromDate']).setValue(start_date)
        date_range.getElement(self._bloom_name_objects['toDate']).setValue(end_date)
        if include_cash is True:
            internal_parameters = request.getElement(self._bloom_name_objects['internalParameters'])
            cash_parameter = internal_parameters.appendElement()
            cash_parameter.getElement(self._bloom_name_objects['name']).setValue('IncludeCash')
            cash_parameter.getElement(self._bloom_name_objects['value']).setValue('Y')

        # Pre-allocate memory
        output_data = _pd.DataFrame(columns=selected_fields)

        # Send the request
        return self._send_request(request, output_data, timeout_seconds, self._process_aim_position_data)

    def aim_cash_service(self, account, account_type, amount_types=None, focus_currencies=None, timeout_seconds=None):
        """Returns cash levels in an account, the data is structured as a DataFrame
            Notes: Requires a Bloomberg Subscription to AIM services
            Inputs:
                - account: the account for which to get the positions
                - account_type: Account or Group
                - amount_types: list, tuple or None, they are the type of cash data required
                - focus_currencies: srt, list, tuple, None, they used as a filter for the type of cash
                - timeout_seconds: float, int the time before the request times-out and is abandoned
            Default Fields:
                - ACCOUNT: if there a multiple accounts requested (through a group) the source account is provided
                - STRATEGY: the strategy of the cash
                - PRIME_BROKER: ???
                - CURRENCY: the currency for cash balance
        """

        # Internal Memory
        amount_types_iterable = False
        focus_currencies_iterable = False
        service_address = '//blp/tsadf'
        request_type = 'RequestCashBalance'

        # Input Validation
        self._input_validation_type(account, 'account', (str,), False)
        self._input_validation_list(account_type, 'account_type', ('ACCOUNT', 'GROUP'), False)
        self._input_validation_type(amount_types, 'amount_types', (str, tuple, list), True)  # Not sure if we can avoid
        if isinstance(amount_types, (tuple, list,)):
            amount_types_iterable = True
        focus_currencies_not_none = self._input_validation_type(focus_currencies, 'focus_currencies',
                                                                (str, tuple, list), False)
        if isinstance(focus_currencies, (tuple, list,)):
            focus_currencies_iterable = True

        # Opening the Service
        if not self._session.openService(service_address):
            raise _BloombergError('Unable to connect to the historical aim positions data service!')
        historical_position_service = self._session.getService(service_address)
        request = historical_position_service.createRequest(request_type)

        # Populate the request
        selected_fields = ['ACCOUNT', 'STRATEGY', 'PRIME_BROKER', 'CURRENCY']
        request.set('entityType', account_type)
        request.set('entityName', account)
        if amount_types is not None:
            if amount_types_iterable is True:
                for amount_type in amount_types:
                    if amount_type != '':
                        request.append('amountType', amount_type)
                        selected_fields.append(amount_type)
            else:
                request.append('amountType', amount_types)
                selected_fields.append(amount_types)
        if focus_currencies_not_none is True:
            if focus_currencies_iterable is True:
                for focus_currency in focus_currencies:
                    if focus_currency != '':
                        request.set('currencyCode', focus_currency)
            else:
                request.set('currencyCode', focus_currencies)

        # Pre-Allocate Memory
        output_data = _pd.DataFrame(columns=selected_fields)

        # Send the request
        self._send_request(request, output_data, timeout_seconds, self._process_aim_cash_service)

    def _listen_bloomberg(self, cid, output_data, call_function):
        """Listens to incoming messages from Bloomberg to catch data returned
            Inputs:
                - cid: correlation id returned on the request
                - output_data: the dataframe to be used for output
                - call_function: the function to be used to process the data
        """
        while True:
            ev = self._session.nextEvent(10)
            for msg in ev:
                msg_keys = msg.correlationIds()
                if cid in msg_keys:
                    output_data = call_function(msg, output_data)
                    if ev.eventType() == _bloom.Event.RESPONSE:
                        return output_data

    def _process_msg_error(self, err_msg):
        """Decomposes the fatal error in a message and raises it as a _BloombergError"""
        err_cat = ''
        err_sub_cat = ''
        err_desc = ''
        if err_msg.hasElement(self._bloom_name_objects['category']):
            err_cat = err_msg.getElement(self._bloom_name_objects['category'])
        if err_msg.hasElement(self._bloom_name_objects['subcategory']):
            err_sub_cat = err_msg.getElement(self._bloom_name_objects['subcategory'])
        if err_msg.hasElement(self._bloom_name_objects['message']):
            err_desc = err_msg.getElement(self._bloom_name_objects['message'])

        raise _BloombergError('{err_cat}: {err_sub_cat}: {err_desc}'.format(err_cat=err_cat,
                                                                            err_sub_cat=err_sub_cat,
                                                                            err_desc=err_desc))

    def _process_field_list_request(self, msg, output_data):
        """Processes a field_list_request data message, returns enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: but populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['fieldData']):
            field_data = msg.getElement(self._bloom_name_objects['fieldData'])
            num_fields = field_data.numValues()
            temp_frame = _pd.DataFrame(index=range(num_fields), columns=['fieldId', 'Mnemonic', 'Description',
                                                                         'DataType', 'Documentation', 'Category',
                                                                         'Overrides'])
            for i in range(0, num_fields):
                data = field_data.getValueAsElement(i)
                field_id = data.getElementAsString(self._bloom_name_objects['id'])
                field_info = data.getElement(self._bloom_name_objects['fieldInfo'])
                data_dict = {'fieldId': field_id,
                             'Mnemonic': field_info.getElementAsString(self._bloom_name_objects['mnemonic']),
                             'Description': field_info.getElementAsString(self._bloom_name_objects['description']),
                             'DataType': field_info.getElementAsString(self._bloom_name_objects['datatype']),
                             'Documentation': field_info.getElementAsString(self._bloom_name_objects['documentation']),
                             'Category': field_info.getElementAsString(self._bloom_name_objects['categoryName'])}
                overrides = field_info.getElement(self._bloom_name_objects['overrides'])
                if overrides.numValues() > 0:
                    overrides_list = []
                    for j in range(overrides.numValues()):
                        overrides_list.append(overrides.getValue(j))
                    data_dict['Overrides'] = str(overrides_list)
                else:
                    data_dict['Overrides'] = None
                temp_frame.iloc[i] = data_dict
            temp_frame.set_index('fieldId', inplace=True)
            output_data = _pd.concat([output_data, temp_frame])
            return output_data

        # Unknown message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_field_info_request(self, msg, output_data):
        """Processes a field_info_request data message, returns enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: but populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['fieldData']):
            field_data = msg.getElement(self._bloom_name_objects['fieldData'])
            num_fields = field_data.numValues()

            for i in range(0, num_fields):
                data = field_data.getValueAsElement(i)
                field_id = data.getElementAsString(self._bloom_name_objects['id'])
                field_info = data.getElement(self._bloom_name_objects['fieldInfo'])
                data_dict = {'fieldId': field_id,
                             'Mnemonic': field_info.getElementAsString(self._bloom_name_objects['mnemonic']),
                             'Description': field_info.getElementAsString(self._bloom_name_objects['description']),
                             'DataType': field_info.getElementAsString(self._bloom_name_objects['datatype']),
                             'Documentation': field_info.getElementAsString(self._bloom_name_objects['documentation'])}
                overrides = field_info.getElement(self._bloom_name_objects['overrides'])
                if overrides.numValues() > 0:
                    overrides_list = []
                    for j in range(overrides.numValues()):
                        overrides_list.append(overrides.getValue(j))
                    data_dict['Overrides'] = str(overrides_list)
                else:
                    data_dict['Overrides'] = None
                properties = field_info.getElement(self._bloom_name_objects['property'])
                if properties.numValues() > 0:
                    for j in range(properties.numValues()):
                        property_element = properties.getValueAsElement(j)
                        if property_element.getElementAsString(self._bloom_name_objects['id']) == 'fieldoverridable':
                            if property_element.getElementAsString(self._bloom_name_objects['value']) == 'true':
                                data_dict['Overridable'] = True
                            else:
                                data_dict['Overridable'] = False

                if data_dict['fieldId'] in output_data.index:
                    ref = data_dict['fieldId']
                else:
                    ref = data_dict['Mnemonic']
                output_data.loc[ref] = data_dict
            return output_data

        # Unknown message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_field_search_request(self, msg, output_data):
        """The returned structure is the same as the field_list_request (but filtered by the searchSpec
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: but populated by the msg data
        """
        return self._process_field_list_request(msg, output_data)

    def _process_reference_data(self, msg, output_data):
        """Processes a reference_data msg, returns enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: but populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['securityData']):
            output_columns = output_data.columns.tolist()
            security_data_array = msg.getElement(self._bloom_name_objects['securityData'])
            for security_data in security_data_array.values():
                ticker = security_data.getElement(self._bloom_name_objects['security']).getValueAsString()
                series = _pd.Series(index=output_columns)
                field_data_array = security_data.getElement(self._bloom_name_objects['fieldData'])
                for field_data in field_data_array.elements():
                    if field_data.isArray():
                        # Bulk Data
                        bulk_data = {}
                        for i in range(0, field_data.numValues()):
                            bulk_element = field_data.getValueAsElement(i)
                            if i == 0:
                                # Populate the keys on initialisation
                                for j in range(0, bulk_element.numElements()):
                                    element = bulk_element.getElement(j)
                                    bulk_data[str(element.name())] = [element.getValue()]
                            else:
                                # Just populate the data
                                for j in range(0, bulk_element.numElements()):
                                    element = bulk_element.getElement(j)
                                    if element.isValid():
                                        bulk_data[str(element.name())].append(element.getValue())
                        series[str(field_data.name())] = bulk_data
                    else:
                        # Non-Bulk Data
                        if field_data.isValid():
                            series[str(field_data.name())] = field_data.getValue()
                # Data Errors
                if security_data.hasElement(self._bloom_name_objects['fieldExceptions']):
                    field_exception_array = security_data.getElement(self._bloom_name_objects['fieldExceptions'])
                    if field_exception_array.numValues() > 0:
                        for field_exception in field_exception_array.values():
                            err_info = field_exception.getElement(self._bloom_name_objects['errorInfo'])
                            series[str(err_info.getElementAsString(self._bloom_name_objects['fieldID']))] = \
                                'ERROR: {}'.format(err_info.getElementAsString('category'))
                # Apply the series to the ticker
                output_data.loc[ticker] = series
            return output_data

        # Unknown Message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_historical_data(self, msg, output_data):
        """Process a historical_data msg, returns enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(_bloom.Name('responseError')))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['securityData']):
            output_columns = output_data.columns.get_level_values('fields').tolist()
            aligned_dates = False
            if 'date' not in output_columns:
                aligned_dates = True
            security_data = msg.getElement(self._bloom_name_objects['securityData'])
            ticker = security_data.getElement(self._bloom_name_objects['security']).getValueAsString()
            field_data_array = security_data.getElement(self._bloom_name_objects['fieldData'])
            df = _pd.DataFrame(index=field_data_array.numValues(), columns=output_columns)
            output_columns = output_columns.append('date')
            series = _pd.Series(index=output_columns)
            counter = 0     # Should match the size of the df DataFrame row-wise
            for field_array in field_data_array.elements():
                for j in range(0, field_array.numElements()):
                    field = field_array.getElement(j)
                    if field.isValid():
                        series[str(field.name())] = field.getValue()
                df.ix[counter] = series
                counter += 1

            # Errors
            if security_data.hasElement(self._bloom_name_objects['fieldExceptions']):
                field_exception_array = security_data.getElement(self._bloom_name_objects['fieldExceptions'])
                for field_exception in field_exception_array.values():
                    err_info = field_exception.getElement(self._bloom_name_objects['errorInfo'])
                    # We apply the error message to the entire vector
                    df.loc[:, str(field_exception.name())] = 'ERROR: {}'.format(err_info.getElementAsString('category'))

            # Apply to the DataFrame to output data
            if aligned_dates is True:
                df.set_index('date', inplace=True)
                output_data.loc[ticker] = df
            else:
                output_data.loc[ticker] = df
            return output_data

        # Unknown Message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_intraday_tick_data(self, msg, output_data):
        """Process intraday tick data messages, returns an enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
            - output_data: populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the Data
        elif msg.hasElement(self._bloom_name_objects['tickData']):
            returned_data = msg.getElement(self._bloom_name_objects['tickData'])
            tick_datas = returned_data.getElement(self._bloom_name_objects['tickData'])
            if tick_datas.numValues() > 0:
                temp_frame = _pd.DataFrame(columns=output_data.columns.tolist(), index=range(0, tick_datas.numValues()))
                for i in range(0, tick_datas.numValues()):
                    tick_data = tick_datas.getValueAsElement(i)
                    dict_data = {'time': tick_data.getElementAsString(self._bloom_name_objects['time']),
                                 'type': tick_data.getElementAsString(self._bloom_name_objects['type']),
                                 'value': tick_data.getElementAsFloat(self._bloom_name_objects['value']),
                                 'size': tick_data.getElementAsInteger(self._bloom_name_objects['size'])}
                    temp_frame.iloc[i] = dict_data
            return _pd.concat([output_data, temp_frame])

        # Unknown Message Type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_aim_position_data(self, msg, output_data):
        """Processes aim position data messages, returns an enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(_bloom.Name('responseError')))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['positionData']):
            position_data = msg.getElement(self._bloom_name_objects['positionData'])
            positions = position_data.getElement(self._bloom_name_objects['dataRow'])
            num_rows = positions.numValues()
            # Pre-allocate space
            row_to_populate = len(output_data.index) + 1
            columns = output_data.columns.tolist()
            output_data = output_data.reindex(index=range(0, len(output_data.index + num_rows)))
            for i in range(0, num_rows):
                position = positions.getValueAsElement(i)
                security = position.getElement(self._bloom_name_objects['securityItem'])
                series = _pd.Series(index=columns)
                series['ACCOUNT'] = security.getElementAsString(self._bloom_name_objects['bookName'])
                series['TICKER'] = security.getElementAsString(self._bloom_name_objects['securityID'])
                series['NAME'] = security.getElementAsString(self._bloom_name_objects['securityName'])

                fields = position.getElement(self._bloom_name_objects['fieldData'])
                if fields.numValues() > 0:
                    for j in range(0, fields.numValues()):
                        field = fields.getValueAsElement(j)
                        field_value = field.getElement(self._bloom_name_objects['data'])
                        if field_value.hasElement(self._bloom_name_objects['S']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldID'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['S'])
                        elif field_value.hasElement(self._bloom_name_objects['D']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldID'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['D'])
                        elif field_value.hasElement(self._bloom_name_objects['I']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldID'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['I'])
                        elif field_value.hasElement(self._bloom_name_objects['B']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldID'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['B'])
                output_data.loc[row_to_populate] = series
                row_to_populate += 1
            return output_data

        # Unknown Message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_aim_historical_position_data(self, msg, output_data):
        """Processes historical aim position data messages, returns an enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['positionHistoryResponse']):  # Maybe needs to be "data" here
            histpos_data = msg.getElement(self._bloom_name_objects['data'])
            num_rows = histpos_data.numValues()
            # Pre-allocate space
            row_to_populate = len(output_data.index) + 1
            columns = output_data.columns.tolist()
            output_data.reindex(index=range(0, len(output_data.index) + num_rows))
            for i in range(0, num_rows):
                position_data = histpos_data.getValueAsElement(i)
                date = position_data.getElementAsDate(self._bloom_name_objects['date'])
                positions = position_data.getElement(self._bloom_name_objects['dataRow'])
                num_values = positions.numValues()
                for j in range(0, num_values):
                    position = positions.getValueAsElement(j)
                    fields = position.getElement(self._bloom_name_objects['fieldData'])
                    series = _pd.Series(index=columns)
                    series['date'] = date
                    for field in fields:
                        field_value = field.getElement(self._bloom_name_objects['data'])
                        if field_value.hasElement(self._bloom_name_objects['S']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldId'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['S'])
                        elif field_value.hasElement(self._bloom_name_objects['D']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldId'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['D'])
                        elif field_value.hasElement(self._bloom_name_objects['I']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldId'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['I'])
                        elif field_value.hasElement(self._bloom_name_objects['B']):
                            series[field.getElementAsString(self._bloom_name_objects['fieldId'])] = \
                                field_value.getElementAsString(self._bloom_name_objects['B'])
                    output_data.loc[row_to_populate] = series
                    row_to_populate += 1
            return output_data

        # Unknown Message type (no error or data)
        else:
            raise _BloombergError('Unknown message type: {}'.format(msg))

    def _process_aim_cash_service(self, msg, output_data):
        """Processes historical aim position data messages, returns an enriched output_data
            Inputs:
                - msg: a Bloomberg Message Object
                - output_data: a pandas DataFrame object
            Outputs:
                - output_data: populated by the msg data
        """
        # Check for fatal errors
        if msg.hasElement(self._bloom_name_objects['responseError']):
            self._process_msg_error(msg.getElement(self._bloom_name_objects['responseError']))

        # Extract the data
        elif msg.hasElement(self._bloom_name_objects['cashBalanceData']):
            cash_data = msg.getElement(self._bloom_name_objects['cashBalanceData'])
            positions = cash_data.getElement(self._bloom_name_objects['dataRow'])
            num_rows = positions.numValues()
            # Pre allocate space
            row_to_populate = len(output_data.index) + 1
            columns = output_data.columns.tolist()
            output_data.reindex(index=range(0, len(output_data.index) + num_rows))
            for i in range(0, num_rows):
                series = _pd.Series(index=columns)
                position = positions.getValueAsElement(i)
                series['ACCOUNT'] = position.getElementAsString(self._bloom_name_objects['account'])
                series['STRATEGY'] = position.getElementAsString(self._bloom_name_objects['strategy'])
                series['PRIME_BROKER'] = position.getElementAsString(self._bloom_name_objects['primeBroker'])
                series['CURRENCY'] = position.getElementAsString(self._bloom_name_objects['currency'])

                fields = position.getElement(self._bloom_name_objects['fieldData'])
                if fields.numValues() > 0:
                    for field in fields:
                        series[field.getElementAsString(self._bloom_name_objects['fieldID'])] = \
                            field.getElementValue(self._bloom_name_objects['settleDateCash'])
                output_data.loc[row_to_populate] = series
                row_to_populate += 1
            return output_data

        # Unknown Message type (no error or data)
        else:
            raise _BloombergError('Unkown message type: {}'.format(msg))


class Asynchronous(Synchronous):
    """Asynchronous has the same requests as Synchronous
        It uses threading to maintain control over the application
        User Functions:
            - request_is_ready: returns True if the request is complete or has failed
            - request_is_error: returns True if the request has an error or timed-out
            - get_request_data: returns the data or False if it is not ready, or raises an error if the request is error
            - all the user functions from the Synchronous class

        Support Functions:
            - _listen_bloomberg: runs as a thread and listens for incoming Bloomberg messages
            - _timeout_monitor: runs as a thread and checks if requests have timed-out
            - _send_request: sends the request and initialises internal memory for this request
            - _clear_request: removes all data from a request
            - _generate_cid: generates a new correlation ID and increments the cid_counter
    """
    # Disabling some inherited Sync requests in the Async class
    field_list_request = property(doc='(!) Disallowed inherited')
    field_info_request = property(doc='(!) Disallowed inherited')
    field_search_request = property(doc='(!) Disallowed inherited')

    def __init__(self, callback_function_removes_data=True):
        # Start the session
        Synchronous.__init__(self)

        # Internal Memory
        self._cid_counter = 0           # Tracks the correlation ID
        self._requests = {}             # {CID: request details}
        self._requests_processing = {}  # {CID: processing function}
        self._requests_data = {}        # {CID: request output_data}
        self._call_functions = {}       # {CID: user call back function}
        self._requests_status = {}      # {CID: READY, PROCESSING, ERROR, TIMEOUT}
        self._requests_timeout = {}     # {CID: {start: dt.datetime, time: time_seconds}}

        # Internal Controls
        self._callback_function_removes_data = callback_function_removes_data   # data removed from Async object after
        # callback function is called
        self._listen_bloomberg_alive = True
        self._timeout_monitor_alive = True

        # Start the threads
        self._thread_listen_bloomberg = _th.Thread(target=self._listen_bloomberg)
        self._thread_listen_bloomberg.daemon = True   # Dies when main thread dies
        self._thread_listen_bloomberg.start()
        self._thread_timeout_monitor = _th.Thread(target=self._timeout_monitor)
        self._thread_timeout_monitor.daemon = True    # Dies when main thread dies
        self._thread_timeout_monitor.start()

    def __del__(self):
        """Once the main thread dies we make sure that the sub-treads are closed properly
            even though they are daemon
        """
        self._listen_bloomberg_alive = False
        self._timeout_monitor_alive = False

    def _listen_bloomberg(self, *args):
        """Overrides the method in Synchronous
            here it constantly listens to incoming Bloomberg messages
            should be a separate thread
        """
        while self._listen_bloomberg_alive:
            ev = self._session.nextEvent(10)
            for msg in ev:
                cids = msg.correlationIds()
                for cid in cids:
                    if cid in self._requests.keys():
                        # Start the processing (in the same thread because of the GIL)
                        try:
                            self._requests_processing[cid](msg, self._requests_data[cid])
                            if ev.eventType() == _bloom.Event.RESPONSE:
                                self._requests_status[cid] = 'READY'
                                if cid in self._call_functions.keys():
                                    # Send the updated status to the call function
                                    self._call_functions[cid](status='READY', cid=cid, data=self._requests_data[cid])
                        except _BloombergError as e:
                            # Some error happened in processing the data
                            self._requests_status[cid] = 'ERROR'
                            self._requests_data[cid] = e.message()
                            if cid in self._call_functions.keys():
                                # Send the updated status to the call function
                                self._call_functions[cid](status='ERROR', cid=cid, data=e.message())

    def _timeout_monitor(self):
        """Keeps checking the time taken by each request and if they are beyond the timeout"""
        while self._timeout_monitor_alive:
            if len(self._requests_timeout) > 0:
                try:
                    for cid in self._requests_timeout.iteritems():
                        start_time = self._requests_timeout[cid]['start']
                        tot_time = _dt.datetime.now() - start_time
                        if tot_time.seconds >= self._requests_timeout[cid]['time']:
                            # Stop the searching of the request
                            self._requests_status[cid] = 'TIMEOUT'
                            if cid in self._call_functions.keys():
                                # Send the updated status to the call function
                                self._call_functions[cid](status='TIMEOUT', data=self._requests_data[cid])
                                self._clear_request(cid)
                except TypeError:
                    # We have deleted a record as a request completed
                    pass

    def _send_request(self, request, output_data, timeout_seconds, call_function):
        """Overrides the method in Synchronous
            here it references the request and returns the cid for the user
        """
        cid = self._generate_cid()
        self._requests[cid] = str(request)
        self._requests_timeout[cid] = {'start': _dt.datetime.now(), 'time': timeout_seconds}
        self._requests_data[cid] = output_data
        self._requests_processing[cid] = call_function
        self._requests_status[cid] = 'PROCESSING'
        cid = self._session.sendRequest(request, correlationId=cid)
        return cid

    def _clear_request(self, cid):
        """Clears the data of a request"""
        del self._requests[cid]
        del self._requests_data[cid]
        del self._requests_status[cid]
        del self._requests_processing[cid]
        if cid in self._requests_timeout.keys():
            del self._requests_timeout[cid]
        if cid in self._call_functions.keys():
            del self._call_functions[cid]

    def _generate_cid(self):
        """Increments the self._cid_counter by 1 and returns the new Correlation ID"""
        self._cid_counter += 1
        return _bloom.CorrelationId(self._cid_counter)

    def request_is_ready(self, cid):
        """Returns true if a given request has finished processing"""
        if cid in self._requests_status.keys():
            if self._requests_status[cid] in ('READY', 'ERROR', 'TIMEOUT'):
                return True
            else:
                return False
        else:
            raise ValueError('The correlation id provided could not be found: cid={}'.format(cid))

    def request_is_error(self, cid):
        """Returns True if a given request has an error or timeout status"""
        if cid in self._requests_status.keys():
            if self._requests_status[cid] in ('ERROR', 'TIMEOUT'):
                return True
            else:
                return False
        else:
            raise ValueError('The correlation id provided could not be found: cid={}'.format(cid))

    def get_request_data(self, cid, remove_data_after_extraction=True):
        """Returns the data is the request is ready, returns False otherwise
            remove_data_after_extraction = True means that you cannot fetch this data by recalling this function
        """
        if self.request_is_ready(cid) is True:
            if self._requests_status[cid] == 'ERROR':
                raise _BloombergError('The request failed!')
            if remove_data_after_extraction is True:
                data = self._requests_data[cid]
                self._clear_request(cid)
                return data
            else:
                return self._requests_data[cid]

    def reference_data(self, tickers, fields, overrides=None, timeout_seconds=None, call_back_function=None):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.reference_data(self, tickers, fields, overrides, timeout_seconds)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid

    def historical_data(self, tickers, fields, start_date, end_date, overrides=None, timeout_seconds=None,
                        call_back_function=None,
                        align_dates=False, periodicity_adjustment=None, periodicity_selection=None, currency=None,
                        override_option=None, pricing_option=None, non_trading_day_fill_option=None,
                        non_trading_day_fill_method=None, max_data_points=None, return_eids=None,
                        return_relative_date=None, adjustment_normal=None, adjustment_abnormal=None,
                        adjustment_split=None, adjustment_follow_dpdf=None, calendar_code_override=None):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.historical_data(tickers, fields, start_date, end_date, overrides, timeout_seconds,
                                          align_dates, periodicity_adjustment, periodicity_selection, currency,
                                          override_option, pricing_option, non_trading_day_fill_option,
                                          non_trading_day_fill_method, max_data_points, return_eids,
                                          return_relative_date, adjustment_normal, adjustment_abnormal,
                                          adjustment_split, adjustment_follow_dpdf, calendar_code_override)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid

    def intraday_tick_data(self, ticker, start_date, end_date, event_types, timeout_seconds=None,
                           call_back_function=None,
                           include_condition_codes=False, include_non_plottable_events=False,
                           include_exchange_codes=False, return_eids=False, include_broker_codes=False,
                           include_rps_codes=False, include_bic_mic_code=False, forced_delay=False,
                           include_spread_price=False, include_yield=False, include_action_codes=False,
                           include_indicator_codes=False, include_trade_time=True, include_upfront_price=False,
                           include_eq_ref_price=False, adjustment_normal=False, adjustment_abnormal=False,
                           adjustment_split=False, adjustment_follow_dpdf=False, include_client_specific_fields=False,
                           include_trade_id=False):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.intraday_tick_data(self, ticker, start_date, end_date, event_types, timeout_seconds,
                                             include_condition_codes, include_non_plottable_events,
                                             include_exchange_codes, return_eids, include_broker_codes,
                                             include_rps_codes, include_bic_mic_code, forced_delay,
                                             include_spread_price, include_yield, include_action_codes,
                                             include_indicator_codes, include_trade_time, include_upfront_price,
                                             include_eq_ref_price, adjustment_normal, adjustment_abnormal,
                                             adjustment_split, adjustment_follow_dpdf, include_client_specific_fields,
                                             include_trade_id)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid

    def aim_position_data(self, account, account_type, fields, include_cash=True, timeout_seconds=None,
                          call_back_function=None):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.aim_position_data(account, account_type, fields, include_cash, timeout_seconds)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid

    def aim_historical_position_data(self, account, account_type, fields, start_date, end_date, include_cash=True,
                                     timeout_seconds=None, call_back_function=None):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.aim_historical_position_data(account, account_type, fields, start_date, end_date,
                                                       include_cash, timeout_seconds)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid

    def aim_cash_service(self, account, account_type, amount_types=None, focus_currencies=None, timeout_seconds=None,
                         call_back_function=None):
        """Overrides the method in Synchronous
            it adds the option to use a call-back function
            returns the correlation id of the request
        """
        cid = Synchronous.aim_cash_service(account, account_type, amount_types, focus_currencies, timeout_seconds)
        if call_back_function is not None:
            self._call_functions[cid] = call_back_function
        return cid


class Subscription(object):
    """A class to manage subscriptions"""
    pass

