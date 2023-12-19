start /min cmd /k "python manage.py empty_all_database_tables"

start /min cmd /k "python manage.py websocket_1"
start /min cmd /k "python manage.py websocket_2"
start /min cmd /k "python manage.py websocket_3"
start /min cmd /k "python manage.py websocket_4"

start /min cmd /k "python manage.py websocket_next_1"
start /min cmd /k "python manage.py websocket_next_2"
start /min cmd /k "python manage.py websocket_next_3"
start /min cmd /k "python manage.py websocket_next_4"

start /min cmd /k "celery -A websocket_trial worker -P solo"
start /min cmd /k "python .\manage.py multiple_task"

start /min cmd /k "python manage.py new_scanner_skew"
start /min cmd /k "python manage.py scanner_pairs"
start /min cmd /k "python manage.py risk_premium_scanner"
start /min cmd /k "python manage.py forward_vol_scanner"
start /min cmd /k "python manage.py idv_cal"

start /min cmd /k "python manage.py position_tracker"

start /min cmd /k "python manage.py live_scanner"




