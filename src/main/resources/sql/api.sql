DEBUG ON;

OPEN QROSS;


SEND MAIL 'Qross System Account'
    FROM TEMPLATE '/templates/notification.html'
    PLACE DATA "username=wuzhneg&password=1234567&host=http://localhost:8080/&fullname=吴铮"
    TO "wuzheng@zichan360.com";