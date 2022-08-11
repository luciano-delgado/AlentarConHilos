import requests, json
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def cambiar_bloqueos_por_api_PRD(pedido,fecha_entrega,destinatario,bloqueonuevo):
    
    headers={'Content-Type': 'application/json','Authorization':"Qk9UWW9pemVuUUE6TU5sa3BvMDk=",'PYTHONHTTPSVERIFY':'0'}
    time.sleep(1)
    jsonDatos = {
          "NRO_PEDIDO":str(pedido),
          "FECHA":str(fecha_entrega),
          "ID_SAP":str(destinatario),
          "BLOQ_PEDIDO":str(bloqueonuevo),
          "TURNO": "",
          "TELEFONO": "",
          "VITA":""
          }
    request = requests.post("https://hor-ewmpo-prd.scienza.com.ar:50001/RESTAdapter/grabarPropuestaPedido",
                            auth=('BOTYoizenPRD', 'ZXasqw12#'),
                            headers=headers,
                            data= json.dumps(jsonDatos),
                            stream=True,
                            verify=False)
    response = request.status_code
    content_response = request.text
    #print(content_response,pedido,bloqueonuevo)

    return content_response


def validar_pedido_PRD(afiliado,padron):
    #payload=json.dump('filter',{'key':"entrega_id",'value':83691438,'operator':"="})
    headers={'Content-Type': 'application/json','Authorization':"Qk9UWW9pemVuUUE6TU5sa3BvMDk=",'PYTHONHTTPSVERIFY':'0'}
    time.sleep(1)
    jsonDatos = {
          "ID_SAP":str(afiliado),
          "PADRON":str(padron),
          }
    
    request = requests.post("https://hor-ewmpo-prd.scienza.com.ar:50001/RESTAdapter/consultaPedidosPendientes",
                            auth=('BOTYoizenPRD', 'ZXasqw12#'),
                            headers=headers,
                            data= json.dumps(jsonDatos),
                            stream=True,
                            verify=False)
    response = request.status_code
    content_response = request.json()

    return content_response['PEDIDOS']

def validar_pedido_QAS(afiliado,padron):
      
      #payload=json.dump('filter',{'key':"entrega_id",'value':83691438,'operator':"="})
      headers={'Content-Type': 'application/json','Authorization':"Qk9UWW9pemVuUUE6TU5sa3BvMDk=",'PYTHONHTTPSVERIFY':'0'}
      time.sleep(1)
      jsonDatos = {
      "ID_SAP":str(afiliado),
      "PADRON":str(padron),
      }

      request = requests.post("https://mdf-poq.scienza.com.ar:50001/RESTAdapter/consultaPedidosPendientes",
                        auth=('BOTYoizenQA', 'MNlkpo09'),
                        headers=headers,
                        data= json.dumps(jsonDatos),
                        stream=True,
                        verify=False)
      response = request.status_code
      content_response = request.json()

      return content_response['PEDIDOS']


def cambiar_bloqueos_por_api_QAS(pedido,fecha_entrega,destinatario,bloqueonuevo):

      
      headers={'Content-Type': 'application/json','Authorization':"Qk9UWW9pemVuUUE6TU5sa3BvMDk=",'PYTHONHTTPSVERIFY':'0'}
      time.sleep(1)
      jsonDatos = {
      "NRO_PEDIDO":str(pedido),
      "FECHA":str(fecha_entrega),
      "ID_SAP":str(destinatario),
      "BLOQ_PEDIDO":str(bloqueonuevo),
      "TURNO": "",
      "TELEFONO": ""
      }
      request = requests.post("https://mdf-poq.scienza.com.ar:50001/RESTAdapter/grabarPropuestaPedido",
                  auth=('BOTYoizenQA', 'MNlkpo09'),
                  headers=headers,
                  data= json.dumps(jsonDatos),
                  stream=True,
                  verify=False)
      response = request.status_code
      content_response = request.text

      return content_response


# ---------------------------

cambiar_bloqueos_por_api_PRD(pedido=5754096,
fecha_entrega='10.08.2022',
destinatario=84000289,
bloqueonuevo="WA")