import requests, json
import time

#Los parametros del HSM deben ser iguales a los nombres de los botones en ySocial!

def comunicar_entrega_ok_con_receta(phone_number,nombre,pedido,entrega):

	plantilla = 'comunicacion_de_entrega_con_envio_receta'
	data = {
    '1': str(nombre),
	'2': str(pedido),
	'3': str(entrega),
	}

	url = 'https://s12.ysocial.net/Scienza/services/messaging/sendwhatsapp'
	jwt = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3UiOjE2NDY3NjI5MDMsImVpIjoxfQ._ZhtktQEuME2elT_fG_xVvIEj3kDWx0hbfc6IH558Nw'
	headers = {
		'Accept' : 'application/json',
		'Authorization' : jwt,
		'Accept': '*/*',
		'Accept-Encoding' : 'gzip, deflate, br',
		'Content-Type' : 'application/json; charset=utf-8',
		'Connection': 'keep-alive'
	}
	data = {
		'serviceId': 2,
	    'phoneNumber': phone_number,
	    'hsm': {
	        'namespace': 'acc0670b_4930_4930_a265_15e7f0b9c20d',
	        'elementName': plantilla,
	        'language': 'es',
	        'header': '',
	        'body': data,
	        'buttons': [
	            {
	                'type': 'quick_reply',
	                'index': 0,
	                'parameter': {
	                    'Receta': str(entrega),
						
	                }
					
	            }
	        ]
	    }
	}
	res = requests.request('POST', url, json=data, headers=headers, verify=False)
	response = res.json()['Success']
	return response

def comunicar_entrega_con_error(phone_number,nombre):

	plantilla = 'error_en_generacion_de_entrega'
	data = {
    '1': str(nombre),
	}


	url = 'https://s12.ysocial.net/Scienza/services/messaging/sendwhatsapp'
	jwt = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3UiOjE2NDY3NjI5MDMsImVpIjoxfQ._ZhtktQEuME2elT_fG_xVvIEj3kDWx0hbfc6IH558Nw'
	headers = {
		'Accept' : 'application/json',
		'Authorization' : jwt,
		'Accept': '*/*',
		'Accept-Encoding' : 'gzip, deflate, br',
		'Content-Type' : 'application/json; charset=utf-8',
		'Connection': 'keep-alive'
	}
	data = {
		'serviceId': 2,
	    'phoneNumber': phone_number,
	    'hsm': {
	        'namespace': 'acc0670b_4930_4930_a265_15e7f0b9c20d',
	        'elementName': plantilla,
	        'language': 'es',
	        'header': '',
	        'body': data,
	        'buttons': [
	            {
	                'type': 'quick_reply',
	                'index': 0,
	                'parameter': {
	                    '1': 'confirm',
						'value':'654321'
	                }
					
	            }
	        ]
	    }
	}
	res = requests.request('POST', url, json=data, headers=headers, verify=False)
	response = res.json()['Success']
	return response

def comunicar_alentar_pedido_para_coordinar(phone_number,nombre,pedido):

	plantilla = 'alentar_pedido_para_coordinar'
	data = {
    '1': str(nombre),
	'2': str(pedido)
	}


	url = 'https://s12.ysocial.net/Scienza/services/messaging/sendwhatsapp'
	jwt = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3UiOjE2NDY3NjI5MDMsImVpIjoxfQ._ZhtktQEuME2elT_fG_xVvIEj3kDWx0hbfc6IH558Nw'
	headers = {
		'Accept' : 'application/json',
		'Authorization' : jwt,
		'Accept': '*/*',
		'Accept-Encoding' : 'gzip, deflate, br',
		'Content-Type' : 'application/json; charset=utf-8',
		'Connection': 'keep-alive'
	}
	data = {
		'serviceId': 2,
	    'phoneNumber': phone_number,
	    'hsm': {
	        'namespace': 'acc0670b_4930_4930_a265_15e7f0b9c20d',
	        'elementName': plantilla,
	        'language': 'es',
	        'header': '',
	        'body': data,
	        'buttons': [
	            {
	                'type': 'quick_reply',
	                'index': 0,
	                'parameter': {
	                    'Pedido': str(pedido)
	                }
					
	            }
	        ]
	    }
	}
	res = requests.request('POST', url, json=data, headers=headers, verify=False)
	response = res.json()['Success']
	return response

def comunicar_feedback_retiro_entrega(phone_number,nombre,entrega,farmacia,calle_f,numero_f,cp_f,localidad_f,prov_f):

	plantilla = 'feedback_retiro_entrega'
	data = {
    '1': str(nombre),
	'2': str(entrega),
	'3': str(farmacia),
	'4': str(calle_f),
	'5': str(numero_f),
	'6': str(cp_f),
	'7': str(localidad_f),
	'8': str(prov_f)
	}


	url = 'https://s12.ysocial.net/Scienza/services/messaging/sendwhatsapp'
	jwt = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3UiOjE2NDY3NjI5MDMsImVpIjoxfQ._ZhtktQEuME2elT_fG_xVvIEj3kDWx0hbfc6IH558Nw'
	headers = {
		'Accept' : 'application/json',
		'Authorization' : jwt,
		'Accept': '*/*',
		'Accept-Encoding' : 'gzip, deflate, br',
		'Content-Type' : 'application/json; charset=utf-8',
		'Connection': 'keep-alive'
	}
	data = {
		'serviceId': 2,
	    'phoneNumber': phone_number,
	    'hsm': {
	        'namespace': 'acc0670b_4930_4930_a265_15e7f0b9c20d',
	        'elementName': plantilla,
	        'language': 'es',
	        'header': '',
	        'body': data,
	        'buttons': [
	            {
	                'type': 'quick_reply',
	                'index': 0,
	                'parameter': {
	                    '2': 'confirm',
						'value':'654321'
	                }
					
	            }
	        ]
	    }
	}
	res = requests.request('POST', url, json=data, headers=headers, verify=False)
	response = res.json()['Success']
	return response

def comunicar_entrega_sin_envio_receta(phone_number,nombre,pedido,entrega):
	plantilla = 'comunicacion_de_entrega_sin_envio_receta'
	data = {
    '1': str(nombre),
	'2': str(pedido),
	'3': str(entrega),
	}
	url = 'https://s12.ysocial.net/Scienza/services/messaging/sendwhatsapp'
	jwt = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3UiOjE2NDY3NjI5MDMsImVpIjoxfQ._ZhtktQEuME2elT_fG_xVvIEj3kDWx0hbfc6IH558Nw'
	headers = {
		'Accept' : 'application/json',
		'Authorization' : jwt,
		'Accept': '*/*',
		'Accept-Encoding' : 'gzip, deflate, br',
		'Content-Type' : 'application/json; charset=utf-8',
		'Connection': 'keep-alive'
	}
	data = {
		'serviceId': 2,
	    'phoneNumber': phone_number,
	    'hsm': {
	        'namespace': 'acc0670b_4930_4930_a265_15e7f0b9c20d',
	        'elementName': plantilla,
	        'language': 'es',
	        'header': '',
	        'body': data,
	        'buttons': [
	            {
	                'type': 'quick_reply',
	                'index': 0,
	                'parameter': {
	                    '1': str(entrega),
	                }
					
	            },
				
				{
	                'type': 'quick_reply',
	                'index': 1,
	                'parameter': {
	                    '2': str(entrega),
	                }
				}

	        ]
	    }
	}
	res = requests.request('POST', url, json=data, headers=headers, verify=False)
	response = res.json()['Success']
	return response


#print("Envio ok con receta",comunicar_entrega_ok_con_receta("5491169576190","Eli","1122","1122"))
# #print("Envio ok sin receta",comunicar_entrega_sin_envio_receta("5491169576190","Lucho","2233","2233"))
# for i in range(0,1):
# 	time.sleep(1)
# 	print("Para alentar pedido",comunicar_alentar_pedido_para_coordinar("5491161162305","Facundo","123456"))
# #print("Envio con error",comunicar_entrega_con_error("5491161162305","Lucho"))


#print("Para alentar pedido",comunicar_alentar_pedido_para_coordinar("5491153368590","Facundo","123456"))
#print("feedback retiro de entrega",comunicar_feedback_retiro_entrega("5491169576190","Facundo","123456","Farmacia Cores","Puan","387","1406","CABA","BUENOS AIRES"))








