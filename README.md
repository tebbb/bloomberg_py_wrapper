bloomberg_py_wrapper
=====

Simplified Python interface to Bloomberg's data.

Prerequisite
=====

[Bloomberg API Sources](https://www.bloomberg.com/professional/support/api-library/) you can also install it via pip: 

    python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
 
 Some of the functions require a subscription to AIM services, they are always preceded by "aim" in their name.
  
Structure
=====

There are three main classes for data requests:
   * Synchronous
   * Asynchronous
   * Subscription (Not Implemented)

The classes Synchronous and Asynchronous have most of their data request types in common.

=> Synchrnous Functions:
   * field_list_request: returns a list of fields matching the field_type (ALL, STATIC, REAL_TIME)
   * field_info_request: returns a description for the searched fields
   * field_search_request: enables the use of filtered search using keywords
   * reference_data: returns reference data or bulk reference data (equivalent of BDP)
   * historical_data: returns historical data, the dates can be alligned (equivalent of BDH)
   * intraday_tick_data: returns intraday tick data
   * aim_position_data: returns the position data of the selected portfolio
   * aim_historical_data: returns the historical position of the data of the selected portfolio
   * aim_cash_service: returns the cash position of the selected portfolio
    
=> Asynchronous Functions (all but the field requests):
   * reference_data: returns reference data or bulk reference data (equivalent of BDP)
   * historical_data: returns historical data, the dates can be alligned
   * intraday_tick_data: returns intraday tick data
   * aim_position_data: returns the position data of the selected portfolio
   * aim_historical_data: returns the historical position of the data of the selected portfolio
   * aim_cash_service: returns the cash position of the selected portfolio
    
=> Subscription (Not Implemented):
   * market_data: can be called-back
    
Notes on Asynchronous
=====

The Asynchronous Class inherits its functions from the Synchronous class with the added benefit that multiple requests can run in the background thus increasing overall speed. 
On a request, instead of returning the data it returns a code that can be used for getting the state of the request (request_is_ready, and request_is_error) as well as recovering the data if it is available (get_request_data). 
An alternative to using the code, is by providing a call_back_function wich must take as inputs: 
   * status: will be either 'READY' or 'ERROR'
   * cid: the code that was returned to you when performing the request
   * data: your data in a pandas dataframe

Notes on Subscripion
=====

Implementation Planed
