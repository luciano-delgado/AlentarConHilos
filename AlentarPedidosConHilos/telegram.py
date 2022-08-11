import requests
    
TOKEN2 = '1337020712:AAFNRvF41xoC7LC_F-nrlr4fVXAFG7bhHbw' #Ponemos nuestro TOKEN generado con el @BotFather
#mi_bot = telebot.TeleBot(TOKEN2)
def bot_send_text(chatid,mensaje):
    send_text = 'https://api.telegram.org/bot' + TOKEN2 + '/sendMessage?chat_id=' + chatid + '&parse_mode=Markdown&text=' + mensaje
    response = requests.get(send_text,verify=False)