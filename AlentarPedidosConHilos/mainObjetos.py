from hdbcli import dbapi
import pandas as pd, numpy as np, urllib3, datetime, time, os, os.path, warnings, json, threading, time, datetime, logging
from pathlib import Path
from api_sap import validar_pedido_PRD, validar_pedido_QAS ,  cambiar_bloqueos_por_api_PRD, cambiar_bloqueos_por_api_QAS
from telegram import bot_send_text
from send import comunicar_alentar_pedido_para_coordinar
from api_gmail import main , enviar_mensaje_adjuntos, enviarMensaje

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning);  warnings.simplefilter(action='ignore', category=UserWarning)
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')



def alentar_pedidos(sacarcobertura,  hsm_por_hilo,  cant_hilos):
    """ENVIAR HSM A LOS AFILIADOS PARA IMPULSAR LA COORDINACIÓN POR VITA"""
    
    
    print(">CARGAR FECHA DEL DIA\n")
    hoy = datetime.datetime.now();  dia=hoy.day;  mes=hoy.month
    
    if dia <= 9:dia = "0" + str(dia)
    else: dia
    if mes <= 9: mes = "0" + str(mes) 
    else: mes
    agno=hoy.year;  hora=hoy.hour;  minutos=hoy.minute
    fh_corrida = str(agno)+'.'+str(mes)+'.'+str(dia)+"_"+str(hora)+"hs_"+str(minutos)+'min'
    
    print(">CARGAR FECHA LIMITE DE ENTREGA\n")
    current_date = datetime.date.today()
    new_date = current_date + datetime.timedelta(20)
    dianuevo = new_date.day
    if dianuevo <= 9: dianuevo = "0" + str(dianuevo)
    else: dianuevo
    mesnuevo = new_date.month
    if mesnuevo <= 9: mesnuevo = "0" + str(mesnuevo)
    else: mesnuevo
    year = new_date.year
    fechaLimite = f"{year}{mesnuevo}{dianuevo}"
    raiz = Path(__file__).resolve().parent

    print(">CARGAR CONEXIÓN A LA BASE DE DATOS\n")
    
    # #QAS
    # conn=dbapi.connect(
    #     address="172.31.0.138",
    #     port="30115",
    #     user="OYP",
    #     password="5tAgt7S8k7XvDx",
    #     sslValidateCertificate=False
    #     )
    # cursor=conn.cursor()
    # cursor.execute("SET SCHEMA SAPABAP1")
    

    #PRD
    conn=dbapi.connect(
        address="172.31.0.130",
        port="30115",
        user="OYP",
        password="A112ShhtPLZYVv",
        sslValidateCertificate=False
        ) ##PRD
    cursor=conn.cursor()
    cursor.execute("SET SCHEMA SAPABAP1") 


    ###CARGAR VARIABLES DE FILTRO PARA LA CONSULTA DE PEDIDOS PENDIENTES
    bloqueos = ('LP','LA','LB','LC','LF','LM','00','M1','M2','M3','N1','N2','N3','N4','PI','PJ','PK','PO','PQ','PT','PV','98','PA','PB','PS','PU','PW','',' ',"")

    clientes_incluir = ('0010000000','0010000305','0010000001','0010000003','0010000004','0010000005','0010000007','0010000008','0010000009','0010000010','0010000012','0010000013',
    '0010000014','0010000015','0010000017','0010000018','0010000019','0010000020','0010000021','0010000023','0010000025','0010000026','0010000027','0010000028','0010000029',
    '0010000030','0010000031','0010000032','0010000033','0010000034','0010000035','0010000036','0010000037','0010000038','0010000039','0010000040','0010000041','0010000049',
    '0010000055','0010000056','0010000057','0010000070','0010000071','0010000072','0010000073','0010000074','0010000076','0010000078','0010000079','0010000115','0010000117',
    '0010000119','0010000130','0010000135','0010000145','0010000157','0020000001','0020000004','0020000005','0020000006','0020000008','0020000010','0020000011','0020000012',
    '0020000014','0020000016','0020000017','0020000028','0020000032','0020000033','0020000038','0020000040','0020000041','0020000043','0020000049','0020000051','0020000052',
    '0020000053','0020000062','0020000066','0020000073','0020000075','0020000076','0020000081','0020000082','0020000084','0020000089','0020000094','0020000098','0020000101',
    '0020000104','0020000110','0020000123','0020000129','0020000156','0020000185','0020000195','0020000205','0020000255','0020000260','0020000261','0020000270','0020000290',
    '0020000290','0020000317','0020000317','0020000346','0020000375','0030000012','0030000455','0040000002','0040000020','0040000121','0010000221','0010000222','0010000223',
    '0010000224','0010000225','0010000226','0010000227','0010000228','0010000229','0010000230','0010000231','0010000232','0010000233','0010000234','0010000235','0010000236',
    '0010000237','0010000238','0010000239','0010000240','0010000241','0010000242','0010000243','0010000244','0010000245','0010000258','0010000265','0010000260','0010000247',
    '0010000248','0010000249','0010000250','0010000251','0010000252','0010000253','0010000254','0010000255','0010000256','0010000266','0010000267','0010000259','0010000262',
    '0010000268','0010000269','0010000264','0010000270','0010000271','0010000272','0010000273','0010000274','0010000275','0010000276','0010000257','0010000277','0010000278',
    '0010000279','0010000280','0010000281','0010000261','0010000282','0010000283','0010000284','0010000285','0010000286','0010000287','0010000288','0010000289','0010000290',
    '0010000291','0010000292','0010000293','0010000294','0010000295','0010000296','0010000297','0010000298','0010000263','0010000299','0010000300','0010000301')

    filiales_osde_sin_stock=('OSDEPB','OSDE CHIVILCOYPB','OSDE TUCUMANPB','OSDE MISIONESPB','OSDE PERGAMINOPB','OSDE BUENOS AIRES CENTROPB','OSDE SAN LUISPB',
    'OSDE TRELEWPB','OSDE TRENQUE LAUQUENPB','OSDE JUJUYPB','OSDE SANTA CRUZPB','OSDE FORMOSAPB','OSDE RIO URUGUAYPB','OSDE ROSARIOPB','OSDE CORDOBAPB','OSDE LA PAMPAPB',
    'OSDE LOBOSPB','OSDE SAN JUANPB','OSDE MAR DEL PLATAPB','OSDE RIO CUARTOPB','OSDE VILLA MARIAPB','OSDE COMODORO RIVADAVIAPB','OSDE BAHIA BLANCAPB',
    'OSDE BARILOCHEPB','OSDE CATAMARCAPB','OSDE CHACOPB','OSDE MENDOZAPB','OSDE SALTAPB','OSDE DEL PARANAPB','OSDE SANTIAGO DEL ESTEROPB',
    'OSDE LA PLATAPB','OSDE LA RIOJAPB','OSDE CORRIENTESPB','OSDE NORPATAGONICAPB','OSDE SANTA FEPB','OSDE TIERRA DEL FUEGOPB','OSDE JUNINPB')

    agentes_invalidos=('04','06','07','08','02','01','03','09')
    columnas_repetidas=("VBPA_VBELN_x","VBUP_VBELN","VBUK_VBELN","VBAP_VBELN","KUNNR","VBPA_VBELN_y")
    porcentaje_afiliado_valido=0.0

    #  provincia_invalida='24'

    print(">GENERAR CONSuLTA DE PEDIDOS PENDIENTES A LA BASE DE DATOS\n")
    query1=f""" 
    SELECT 
    VBUP.VBELN AS VBUP_VBELN,    
    VBUK.VBELN AS VBUK_VBELN,
    VBAP.POSNR AS POSICION_PEDIDOS,
    VBAP.MATNR AS ID_MATERIAL,
    VBAP.VBELN AS VBAP_VBELN,
    MARA.ZZTXT AS DESCRIPCION_MATERIAL, 
    MARA.ZZESTADO AS ESTADO_MATERIAL,
    ZMM_ESTADOST.DESCRIPCION  AS ESTADO_DESC,
    VBAK.ERDAT AS FECHA_CREACION_PEDIDO,
    VBAK.VDATU AS FECHA_ENTREGA_PEDIDO,
    VBAK.KUNNR,
    VBAK.VBELN AS PEDIDO,
    ZSD_CONVENIOS.DESCRIPCION AS DESCRIPCION_CONVENIO,
    ZSD_CONVENIOS.CONVENIO AS ID_CONVENIO,
    ZSD_CONVENIOS.PORC_AFIL AS AFILIADO_PORCENTAJE,
    KNA1.NAME1 AS DESCRIPCION_CLIENTE,
    KNA1.KUNNR AS ID_CLIENTE,
    VBAK.LIFSK AS BLOQUEO_PEDIDO

    FROM   
    (VBUP
    inner join VBUK 
    on  VBUK.VBELN = VBUP.VBELN
    inner join VBAP
    on  VBAP.POSNR = VBUP.POSNR
    and VBAP.VBELN = VBUP.VBELN
    inner join MARA
    on  MARA.MATNR = VBAP.MATNR
    inner join VBAK
    on  VBAK.VBELN = VBAP.VBELN
    inner join ZSD_CONVENIOS
    on  ZSD_CONVENIOS.CONVENIO = VBAK.ZZCONVENIO
    inner join KNA1
    on  KNA1.KUNNR = VBAK.KUNNR 
    INNER JOIN ZMM_ESTADOST ON ZMM_ESTADOST.ESTADO = MARA.ZZESTADO)

    WHERE 
    (VBUP.LFSTA = 'A' OR VBUP.LFSTA = 'B') AND 
    (VBUP.ABSTA = 'A' OR VBUP.ABSTA = 'B') AND
    (VBAK.AUART = 'ZTER' OR VBAK.AUART = 'ZTRA' OR VBAK.AUART = 'ZLIA') AND
    (VBAK.VDATU <= {fechaLimite}) AND               /* FH_PREF_ENTREGA de acá a 20 dias max */
    (VBAK.ERDAT >= 20211101)                        /* CREADO_EL mayor a 01.11.2021 */
    """

    cursor.execute(query1)
    df1 = pd.read_sql_query(query1,conn);  print(df1.shape); print(df1.head(10))

    l_pedidos = []
    for i in range(0,10000000):
        try:
            ped = df1.loc[i,'VBUP_VBELN'] #Pedidos Pendientes
            l_pedidos.append(ped)
        except:
            break
    ids_pedidos = (', '.join("'" + item + "'" for item in l_pedidos))

    l_clientes = []
    for i in range(0,10000000):
        try:
            ped = df1.loc[i,'ID_CLIENTE']
            l_clientes.append(ped)
        except:
            break
    ids_solicitante = (', '.join("'" + item + "'" for item in l_clientes))

    print(">GENERAR CONSULTA DE AFILIADOS RELACIONADOS A LOS PEDIDOS PENDIENTES\n")
    query2 = """
    SELECT 
    VBPA.KUNNR AS ID_AFILIADO,
    VBPA.VBELN AS VBPA_VBELN,
    KNA1.NAME1 AS AFILIADO_NOMBRE
    FROM (VBPA
    INNER JOIN KNA1
    ON VBPA.KUNNR = KNA1.KUNNR)

    WHERE VBPA.PARVW = 'ZA' AND VBPA.VBELN IN (%s)
    """ %(ids_pedidos)  
    cursor.execute(query2)
    df2 = pd.read_sql_query(query2,conn)
    l_afiliados = []
    for i in range(0,10000000):
        try:
            ped = df2.loc[i,'ID_AFILIADO']
            l_afiliados.append(ped)
        except:
            break
    ids_afiliados = (', '.join("'" + item + "'" for item in l_afiliados))

    print(">GENERAR CONSULTA DE DESTINATARIOS DE PEDIDOS RELACIONADOS A LOS PEDIDOS PENDIENTES\n")
    query2_1 = """
    SELECT 
    VBPA.KUNNR AS ID_DESTINATARIO,
    VBPA.VBELN AS VBPA_VBELN,
    KNA1.ZZAGENTE AS AGENTE_DESTINATARIO,
    KNA1.PSTLZ AS CODIGO_POSTAL_DESTINATARIO,
    KNA1.REGIO AS PROVINCIA_DESTINATARIO

    FROM (VBPA
    inner join KNA1
    on KNA1.KUNNR = VBPA.KUNNR)

    WHERE VBPA.PARVW = 'WE' AND VBPA.VBELN IN (%s)
    """%(ids_pedidos) 
    df2_1 = pd.read_sql_query(query2_1,conn)

    print(">GENERAR CONSULTA DE TELEFONO DE AFILIADOS\n") 
    query4="""
    SELECT
    ZSD_AFI_TEL.KUNNR AS ID_AFILIADO_TELEFONO,
    ZSD_AFI_TEL.CEL_COD_AREA AS CEL_COD_AREA,
    ZSD_AFI_TEL.CEL_INTERNO AS CEL_COD_INICIAL,
    ZSD_AFI_TEL.CEL_TELEFONO AS CEL_COD_TELEFONO

    FROM ZSD_AFI_TEL

    WHERE ZSD_AFI_TEL.KUNNR IN (%s)
    """%(ids_afiliados)
    cursor.execute(query4)
    df4 = pd.read_sql_query(query4,conn)

    print(">GENERAR CONSULTA DE PADRON DE SOLICITANTES\n") 
    query5="""
    SELECT
    ZBUT0ID.TYPE AS TIPO_DE_PADRON,
    ZBUT0ID.PARTNER AS SOLICITANTE_DEL_PADRON,
    ZBUT0ID.IDNUMBER AS NUMERO_DE_PADRON

    FROM ZBUT0ID
    
    WHERE ZBUT0ID.TYPE = 'ZPADRO' AND ZBUT0ID.PARTNER IN (%s)
    """%(ids_solicitante)
    df5 = pd.read_sql_query(query5,conn)

    
    print(">QUITAR TODO PEDIDO QUE TENGA AGENTE INVALIDOS\n")
    for agente in agentes_invalidos:
        indexNames = df2_1[ df2_1['AGENTE_DESTINATARIO'] == agente].index
        df2_1.drop(indexNames , inplace=True)  

    
    print(">COMBINAR LOS PEDIDOS PENDIENTES CON SUS AFILIADOS\n")
    df12 = pd.merge(left=df1, right=df2, left_on='PEDIDO', right_on='VBPA_VBELN')
    
    print(">COMBINAR LOS PEDIDOS PENDIENTES CON AFILIADOS Y SUS DESTINOS\n")
    df13 = pd.merge(left=df2_1, right=df12, left_on='VBPA_VBELN', right_on='PEDIDO')
    print(df13.shape)
   
    print(">COMBINAR LOS PEDIDOS CON LOS TELEFONOS DE LOS AFILIADOS\n")
    df14 = pd.merge(left=df13, right=df4,how='left', left_on='ID_AFILIADO', right_on='ID_AFILIADO_TELEFONO')
    print(df13.shape)

    print(">COMBINAR LOS PEDIDOS CON LOS PADRONES DE LOS AFILIADOS\n")
    df15 = pd.merge(left=df14, right=df5,how='left', left_on='ID_CLIENTE', right_on='SOLICITANTE_DEL_PADRON')
    print(df13.shape)
    raiz = Path(__file__).resolve().parent
    ruta_excel_1 = str(raiz)+"/UniversosinDepurar.xlsx"
    df15.to_excel(ruta_excel_1)
    
    print(">QUITAR LOS PEDIDOS PENDIENTES DE OSDE INTERIOR Y OSDE METRO QUE TENGAN BLOQUEO PB\n")
    df15['CLIENTEBLOQUEO']=df15['DESCRIPCION_CLIENTE'] + df15['BLOQUEO_PEDIDO']
    for filiales in filiales_osde_sin_stock:
        indexName = df15[df15['CLIENTEBLOQUEO']==filiales].index
        df15.drop(indexName, inplace=True)
    print(df15.shape)


    # ###QUITAR LOS PEDIDOS DE TIERRA DEL FUEGO ------------------------ELIMINADO POR JULI FRANCO 30/3
    # print(">QUITAR LOS PEDIDOS DE TIERRA DEL FUEGO\n")
    # try:
    #     indexName = df15[df15['PROVINCIA_DESTINATARIO']==provincia_invalida].index
    #     df15.drop(indexName, inplace=True)
    # except:
    #     pass
    # print(df15.shape)


    ###QUITAR TODOS LOS PEDIDOS QUE TENGAN % DE AFILIADO MAYOR A CERO----------------------ELIMINADO POR JULI FRANCO 30/3----------VUELTO A PONER POR LORE EL 11/4
    if sacarcobertura == "SI":
        print(">QUITAR TODOS LOS PEDIDOS QUE TENGAN % DE AFILIADO MAYOR A CERO\n")
        indexName = df15[df15['AFILIADO_PORCENTAJE']!=porcentaje_afiliado_valido].index
        df15.drop(indexName, inplace=True)
        print(df15.shape)
    else:
        pass
    #df15.to_excel(r"C:\Users\ldigital20\Desktop\alentarpedido\antes_de_quitar_bloqueo.xlsx")


    print(">QUITAR TODOS LOS PEDIDOS QUE TENGAN BLOQUEOS NO COORDINABLES\n")
    for index,row in df15.iterrows():
        if row[22] not in bloqueos:
            df15.drop(index,inplace=True)
        else:
            continue
    print(df15.shape)
    
    print(">QUITAR TODOS LOS PEDIDOS QUE TENGAN CLIENTES QUE NO REQUIERAN COORDINACION\n")
    for index,row in df15.iterrows():
        if row[21] not in clientes_incluir:
            df15.drop(index,inplace=True)
        else:
            continue
    print(df15.shape)

    print(">QUITAR TODOS LOS PEDIDOS QUE NO TENGAN TELEFONO\n")
    df15['CEL_COD_TELEFONO']=df15['CEL_COD_TELEFONO'].astype(str)
    indexName = df15[df15['CEL_COD_TELEFONO']=="nan"].index
    df15.drop(indexName, inplace=True)
    indexName = df15[df15['CEL_COD_TELEFONO']==""].index
    df15.drop(indexName, inplace=True)
    print(df15.shape)

    ###CONFIRMAR TELEFONO
    df15['CEL_COD_INICIAL'].astype(str)
    df15['CEL_COD_TELEFONO'].astype(str)
    df15['DIGITOINICIAL']='549'
    df15["TELEFONO_VALIDO"]=df15['DIGITOINICIAL'] + df15['CEL_COD_INICIAL'] + df15['CEL_COD_TELEFONO']

    print(">ELIMINAR COLUMNAS REPETIDAS \n")
    for columnas in columnas_repetidas:
        del(df15[columnas])

    print(f'Universo Inicial Depurado --> {df15.shape}')
    #ruta_excel_1 = str(raiz)+"/UniversoInicialDepuradoORIGINAL.xlsx"
    df15.to_excel(ruta_excel_1)
    print(">CONSULTAR PEDIDOS, ENVIAR HSM Y CAMBIAR BLOQUEO\n")#f'--- INICIANDO PROCESO DE ENVIO DE {cant_hsm} HSM --- ')
    df15['ID_AFILIADO'].astype(int)
    #df15_original = df15


    # #FILTRO VALORES PARA PRUEBA
    # df15 = df15.loc[df15['AFILIADO_NOMBRE'].isin([

    #     'MARTA VERONICA FERNANDEZ', # 1569576190 (Lucho)
    #     'CARLA CAMPOPIANO', # 1161162305 (Facu) 
    #     'LUCIANA PAOLA GONZALEZ', # 1126312338 (Joshu) 
    #     'JORGE LUIS LOPEZ', # 1153368590 (Deca) 
    #     'juan pedro', # 1161285737 (Olga) 
    #     'LLEWELYN KAST', #  1170078063 (Adri) 
    #     'HECTOR DANIEL NIRENBERG', #  1135378749 (bazan)
    #     'SERGIO YEBRIN', #  1149701912 (furlani)
    #     'MARTA OFELIA ORIETA', #  1157371346 (laguzzi)
    #     'SILVIA RUBIOLO', #  1169463343 (caro)
    #     ])]
    
    # print("\n\n\n**********************AFILIADOS CON TELEFONO PARA PRUEBAS ********************\n\n")
    # print(df15[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]])

    
    df15.sort_values(by='ID_AFILIADO',ascending=True,inplace=True) # ------------------------------ 
    raiz = Path(__file__).resolve().parent
    #ruta_excel_2 = str(raiz)+"/Pedidos_UniversoInicialDepurado_pruebas.xlsx"
    ruta_excel_2 = str(raiz)+"/Pedidos_UniversoInicialDepurado.xlsx"
    df15.to_excel(ruta_excel_2)

    #afiliadoanterior=""
    #cantidaddehsm=0
    #cantidaddepedidoscoordinados=0
    #cantidaddelinasanalizadas=0
    archivo=open(str(raiz)+"/log.txt","wt")
    #archivo=open(str(raiz)+f"/log__{fh_corrida}.txt","wt")

    l_pedidos_wa = [] ; l_pedidos_no_wa = [];  l_afiliados_wa = [] ; l_afiliados_no_wa = [] 

    # Defino una cota para subdividir lineas del df_15 (lo divido en 10)
    cota = round(len(df15.index)/cant_hilos)    

    m = f'\n<<< Cant de posiciones universo inicial depurado {len(df15.index)} - Grupos de: {cota} posiciones>>>\n';  print(m); archivo.write(m)

    # Clase Hilo
    class ThreadWithResult(threading.Thread):
        def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
            def function():
                self.result = target(*args, **kwargs)
            super().__init__(group=group, target=function, name=name, daemon=daemon)


    # Exploto el df15 en 10 "df15_..." 
    df15_1 = df15.iloc[:cota];  print(f'\n\ndf15_1 shape {df15_1.shape}:\n {df15_1[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_1: "+str(df15_1.shape)+"\n")
    df15_2 = df15.iloc[cota:cota*2];  print(f'\n\ndf15_2 {df15_2.shape}:\n {df15_2[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_2: "+str(df15_2.shape)+"\n")
    df15_3 = df15.iloc[cota*2:cota*3];  print(f'\n\ndf15_3 {df15_3.shape}:\n {df15_3[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_3: "+str(df15_3.shape)+"\n")
    df15_4 = df15.iloc[cota*3:cota*4];  print(f'\n\ndf15_4 {df15_4.shape}:\n {df15_4[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_4: "+str(df15_4.shape)+"\n")
    df15_5 = df15.iloc[cota*4:cota*5];  print(f'\n\ndf15_5 {df15_5.shape}:\n {df15_5[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_5: "+str(df15_5.shape)+"\n")
    df15_6 = df15.iloc[cota*5:cota*6];  print(f'\n\ndf15_6 {df15_6.shape}:\n {df15_6[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_6: "+str(df15_6.shape)+"\n")
    df15_7 = df15.iloc[cota*6:cota*7];  print(f'\n\ndf15_7 {df15_7.shape}:\n {df15_7[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_7: "+str(df15_7.shape)+"\n")
    df15_8 = df15.iloc[cota*7:cota*8];  print(f'\n\ndf15_8 {df15_8.shape}:\n {df15_8[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_8: "+str(df15_8.shape)+"\n")
    df15_9 = df15.iloc[cota*8:cota*9];  print(f'\n\ndf15_9 {df15_9.shape}:\n {df15_9[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_9: "+str(df15_9.shape)+"\n")
    df15_10 = df15.iloc[cota*9:];  print(f'\n\ndf15_10 {df15_10.shape}: {df15_10[["AFILIADO_NOMBRE","ID_AFILIADO","PEDIDO","TELEFONO_VALIDO"]]}');  archivo.write("df15_10: "+str(df15_10.shape)+"\n")
    # df15_1.to_excel('df15_1.xlsx')
    # df15_2.to_excel('df15_2.xlsx')
    # df15_3.to_excel('df15_3.xlsx')
    # df15_4.to_excel('df15_4.xlsx')
    # df15_5.to_excel('df15_5.xlsx')
    # df15_6.to_excel('df15_6.xlsx')
    # df15_7.to_excel('df15_7.xlsx')
    # df15_8.to_excel('df15_8.xlsx')
    # df15_9.to_excel('df15_9.xlsx')
    # df15_10.to_excel('df15_10.xlsx')
    
    m = f'\n\n .............COMIENZAN LOS {cant_hilos} HILOS CON {hsm_por_hilo} HSM COMO MAXIMO C/U................\n\n' ;  print(m)  ;archivo.write(m) 
    l_pedidos_error_cambio_bloqueo = []

    def iterar_df15_y_enviar_hsm(df_ingresado,hilo,max_hsm_por_hilo):
        
        afiliadoanterior="";  cantidaddehsm=0;  cantidaddelinasanalizadas=0;  cantidaddepedidoscoordinados=0

        for index,row in df_ingresado.iterrows():
            
            cantidaddelinasanalizadas+=1;  afiliado=row[18]
            
            try:
                if afiliadoanterior!=afiliado and cantidaddepedidoscoordinados < max_hsm_por_hilo:
                    
                    nombre_afiliado=row[19];  padron=row[26];  telefono=row[29];  bloqueo_pedido_anterior = row[17]

                    try:
                        m = f'\n\nHilo[{hilo}] ANALISIS AFILIADO {afiliado} - Padron: {padron}'; logging.info(m);  archivo.write(m) 
                        
                        resultado=validar_pedido_PRD(afiliado,padron)
                        #resultado=validar_pedido_QAS(afiliado,padron)

                        l_resultados = []
                        for i in range(0,100):
                            try:l_resultados.append(resultado[i]["PEDIDO"])
                            except:break

                        m = f'\n\tHilo[{hilo}] Hay {len(l_resultados)} Pedidos Coordinables por Vita para {nombre_afiliado} (ID: {afiliado} Padron: {padron}):\n\t{l_resultados}' ;  logging.info(m);  archivo.write(m) 

                        for i in resultado: 
                            if cantidaddepedidoscoordinados < max_hsm_por_hilo: # {AGREGADO}
                                try:
                                    
                                    filtro=df_ingresado[df_ingresado['PEDIDO']=="000" + str(i['PEDIDO'])]
                                    pedido=str(i['PEDIDO'])
                                    m = f'\n\tHilo[{hilo}] Analizo pedido: {pedido}';  logging.info(m);  archivo.write(m) 
                                    filtro.reset_index(inplace=True, drop=True)
                                    fechadeentrega=filtro.loc[0,"FECHA_ENTREGA_PEDIDO"]
                                    fentrerealfinal=fechadeentrega[6:8] + "." + fechadeentrega[4:6] + "." + fechadeentrega[0:4]
                                    destinatario=filtro.loc[0,"ID_DESTINATARIO"]

                                    m =f'\n\t\tHilo[{hilo}] VALIDO COORDINABLE >>>>>\n\t\t\t\t\t\tAFILIADO {afiliado}\n\t\t\t\t\t\t\tPEDIDO: {pedido}\n\t\t\t\t\t\t\t\tFECHA DE ENTREGA: {fentrerealfinal}\n\t\t\t\t\t\t\t\tDESTINATARIO: {destinatario}\n\t\t\t\t\t\t\t\tBQ: WA\n\t\t\t\t\t\t\t\tTEL: {telefono}'; logging.info(m);  archivo.write(m)
                                    
                                    if telefono == '5491135030782' or telefono == 5491135030782: print("No enviar hsm a este tel")
                                    
                                    #ENVIO HSM Y CAMBIO BQ A WP/WA
                                    else:

                                        m = f'\n\t\t\t\t\t\t\t\tENVIANDO HSM A {nombre_afiliado} - {telefono}... (COMENTADO)';  logging.info(m);  archivo.write(m) 

                                        comunicar_alentar_pedido_para_coordinar(telefono,nombre_afiliado,pedido)
                                        
                                        if pedido not in l_pedidos_error_cambio_bloqueo:

                                            try:
                                                if bloqueo_pedido_anterior == "LP":
                                                    m = f'\n\t\t\t\t\t\t\t\tPedido LP --> Se modifica a WP (COMENTADO)\n';  logging.info(m);  archivo.write(m) 
                                                    cambiar_bloqueos_por_api_PRD(pedido,fentrerealfinal,destinatario,"WP")
                                                    
                                                else:
                                                    m = f'\n\t\t\t\t\t\t\t\tPedido No LP --> Se modifica a WA (COMENTADO)\n'; logging.info(m);  archivo.write(m) 
                                                    cambiar_bloqueos_por_api_PRD(pedido,fentrerealfinal,destinatario,"WA")
                                            except: 
                                                enviar_mensaje_adjuntos = f'Ha ocurrido un error al cambiar el bloqueo de pedido {pedido}.\nLista de pedidos con error: {l_pedidos_error_cambio_bloqueo}'
                                                l_pedidos_error_cambio_bloqueo.append(pedido)
                                                print(mensaje)
                                                servicio = main()
                                                enviarMensaje(servicio, mensaje)
                                                pass
                                            

                                        pedido = '000'+str(pedido)
                                        l_afiliados_wa.append(afiliado)
                                        l_pedidos_wa.append(pedido) 
                                        cantidaddepedidoscoordinados+=1
                                        result = cantidaddepedidoscoordinados

                                except Exception as exception1:
                                    pedido=str(i['PEDIDO'])  
                                    m = f'\n\t\t\tHilo[{hilo}] no aplica para hsm! > Pedido: {pedido}, Afiliado: {afiliado} Coordinable por Vita pero no por envio saliente (Puede ser tel invalido o % afi > 0). Detalle [Error1: {exception1}]\n';  logging.info(m);  archivo.write(m)     
                                    continue
                            
                            else: pass
        
                    except Exception as exception2:
                        l_afiliados_no_wa.append(afiliado)   
                        afiliadoanterior=afiliado
                        cantidaddehsm+=1  #cantidaddehsm = cantidad de afiliados analizados
                        m =f'\n\t\t\tHilo[{hilo}] no valido! > Afiliado {afiliado} Sin pedidos coordinables.Detalle [Error2: {exception2}].\n\tCANTIDAD de AFILIADOS ANALIZADOS en [hilo {hilo}] = {cantidaddehsm} - PEDIDOS COORDINADOS = {cantidaddepedidoscoordinados}'; logging.info(m);  archivo.write(m)     
                        continue

                    afiliadoanterior=afiliado
                    cantidaddehsm+=1
                    m = f'\nHilo[{hilo}] CANTIDAD de AFILIADOS ANALIZADOS en [hilo{hilo}] = {cantidaddehsm} - PEDIDOS COORDINADOS = {cantidaddepedidoscoordinados}'; logging.info(m);  archivo.write(m)     

                elif afiliadoanterior!=afiliado and cantidaddepedidoscoordinados >= max_hsm_por_hilo:
                
                    m = f"\n\t\t\t\t\t\t~~~ HSM POR HORA SUPERADA en [Hilo{hilo}]. Se enviaron {cantidaddepedidoscoordinados} salientes~~~\n"; logging.info(m);  archivo.write(m)     
                    break

            except Exception as exception3:
                afiliadoanterior=afiliado
                cantidaddehsm+=1
                m = f'\n\t\t\tHilo[{hilo}] incoordinable > Afiliado {afiliado} con Error3 {exception3} \n\t\t\tCANTIDAD de AFILIADOS ANALIZADOS en [hilo {hilo}] = {cantidaddehsm} - PEDIDOS COORDINADOS =  {cantidaddepedidoscoordinados}'; logging.info(m);  archivo.write(m) 
                
                bot_send_text("1202513315",f'Facundo sali por este error:  {exception3}') 
                continue
        
        return cantidaddepedidoscoordinados
            
    
    h1 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo1",  args=(df15_1,'1',hsm_por_hilo));  h1.start()
    h2 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo2",  args=(df15_2,'2',hsm_por_hilo));  h2.start()
    h3 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo3",  args=(df15_3,'3',hsm_por_hilo));  h3.start()
    h4 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo4",  args=(df15_4,'4',hsm_por_hilo));  h4.start()
    h5 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo5",  args=(df15_5,'5',hsm_por_hilo));  h5.start()
    h6 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo6",  args=(df15_6,'6',hsm_por_hilo));  h6.start()
    h7 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo7",  args=(df15_7,'7',hsm_por_hilo));  h7.start()
    h8 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo8",  args=(df15_8,'8',hsm_por_hilo));  h8.start()
    h9 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo9",  args=(df15_9,'9',hsm_por_hilo));  h9.start()
    h10 = ThreadWithResult(target=iterar_df15_y_enviar_hsm, name = "Hilo10",  args=(df15_10,'10',hsm_por_hilo));  h10.start()
    
    h1.join();  h2.join();  h3.join();  h4.join();  h5.join();  h6.join();  h7.join();  h8.join();  h9.join();  h10.join()
    
    result = h1.result + h2.result + h3.result + h4.result + h5.result + h6.result + h7.result + h8.result + h9.result + h10.result
    
    m = f'|Total hsm enviados: {result}|';  print(m);  archivo.write(m)
    
    
    # ARMO RESUMEN FINAL --------------------------------------------------------------
   
    np1 = np.array(l_afiliados_wa)
    np2 = np.array(l_pedidos_wa)
    
    df16 = pd.DataFrame(np2,np1,columns=['Afiliado de Pedido'])
    print(f'\n\n> Pedidos coordinables: Afiliado + Pedido (Conjunto A)'+"\n",df16.head(10)); 
    df16.columns = ['PEDIDO']
    df16.to_excel(str(raiz)+"/PedidosCoordinados.xlsx")
    
    df17 = pd.merge(df15, df16, on=['PEDIDO'], how="outer", indicator=True)
    df17 = df17[df17['_merge'] == 'left_only']

    print(f'>\n\n PEDIDOS NO COORDINADOS (ANALIZADOS y NO ANALIZADOS - Conjuntos B+C)');  print(df17.shape);  print(df17.head(5))
    df17.to_excel(str(raiz)+"/PedidosNoCoordinadosTotales.xlsx")
    
    np3=np.array(l_afiliados_no_wa)
    df_afiliados_no_wa = pd.DataFrame(np3);print("\n\n > AFILIADOS DE PEDIDOS NO COORDINABLES");  print(df_afiliados_no_wa.head(10));  
    df_afiliados_no_wa.columns = ['ID_AFILIADO']
    
    print("\n\n > MEZCLO AFILIADOS DE PEDIDOS NO COORDINABLES CON df15 - Conjunto B")
    df18 = pd.merge(df15, df_afiliados_no_wa, on=['ID_AFILIADO'], how="inner", indicator=True);  print(df18.head(10));  
    
    m1 =f'\n\nPosiciones analizadas y no coordinadas {df18.shape}';  print(m1),  archivo.write(m1)
    df18.to_excel(str(raiz)+"/PedidosNoCoordinablesAnalizados.xlsx")

    ###FIN
    bot_send_text("1202513315",f'Facundo ya terminé de mandar los HSM:\nPEDIDOS COORDINADOS: {result}\n')    

    # Envio mail informando pedidos en conjunto B (Analizados y no coordinados)
    hsm_totales = hsm_por_hilo * cant_hilos
    
    try:
        servicio = main() 
        enviar_mensaje_adjuntos(
            file = '/AlentarPedidos/PedidosNoCoordinablesAnalizados.xlsx',
            service = servicio,
            pos_no_coordinables = len(df18.index),
            hsm_programado = hsm_totales, 
            hsm_programadohilo = hsm_por_hilo,
            hsm_enviados = result,
            cantidaddehilos = cant_hilos
        )

        m = f'\n\n\nMail enviado con PedidosNoCoordinables - {len(df18.index)} posiciones';  archivo.write(m) 
    
    except Exception as exc:
        print(exc), print("No pude enviar mail PedidosNoCoordinables");  archivo.write(m) 
        pass
    
    archivo.close();  cursor.close();  conn.close()
    print("--"*50,"Fin")

# -----------------------------------------------------------------------------------------------------------
if __name__=="__main__":    
    diadelasemana=datetime.datetime.today().weekday()
    fecha_y_hora_actual=time.ctime()
    hora_actual=fecha_y_hora_actual.split()
    listadehora=hora_actual[3:4]
    solo_hora=listadehora[0][0:2]
    
    # Domingo
    if diadelasemana==6 and solo_hora=="10":
        cantidad=800
        bot_send_text("1202513315",f'Facundo se activo el bot a las 11am con {cantidad} hsm por ser domingo')
        
        alentar_pedidos(sacarcobertura = "SI", hsm_por_hilo = 80, cant_hilos = 10)
    
    #Sábado
    elif diadelasemana==5 and solo_hora=="07":
        for i in range(0,4):
            cantidad=400
            bot_send_text("1202513315",f'Facundo se activo el bot a las 07 del sabado para mandar {cantidad} hsm')
            
            alentar_pedidos(sacarcobertura = "NO", hsm_por_hilo = 40, cant_hilos = 10)
    
    #Lunes a Viernes
    elif (diadelasemana==0 or diadelasemana==1 or diadelasemana==2 or diadelasemana==3 or diadelasemana==4) and (solo_hora=="11"):
    #elif (diadelasemana==0 or diadelasemana==1 or diadelasemana==2 or diadelasemana==3 or diadelasemana==5) and (solo_hora=="3.14"): #HARDCODEADO PARA PRUEBAS
        cantidad = 150
        bot_send_text("1202513315",f'Facundo se activo el bot de las 07 para mandar {cantidad} hsm')
        alentar_pedidos(sacarcobertura = "SI", hsm_por_hilo = 15, cant_hilos = 10)

        while (solo_hora != "20") and (solo_hora != "21") and (solo_hora != "22") and (solo_hora != "23"):
            cantidad=350
            bot_send_text("1202513315",f'Facundo se activo el primer ciclo del bot para mandar {cantidad} hsm')
            
            alentar_pedidos(sacarcobertura = "NO", hsm_por_hilo = 35, cant_hilos = 10)
            diadelasemana=datetime.datetime.today().weekday()
            fecha_y_hora_actual=time.ctime()
            hora_actual=fecha_y_hora_actual.split()
            listadehora=hora_actual[3:4]
            solo_hora=listadehora[0][0:2]

        while (solo_hora == "20") or (solo_hora == "21"):
            cantidad=250
            bot_send_text("1202513315",f'Facundo se activo el segundo ciclo del bot para mandar {cantidad} hsm')
            alentar_pedidos(sacarcobertura = "SI", hsm_por_hilo = 25, cant_hilos = 10)
            diadelasemana=datetime.datetime.today().weekday()
            fecha_y_hora_actual=time.ctime()
            hora_actual=fecha_y_hora_actual.split()
            listadehora=hora_actual[3:4]
            solo_hora=listadehora[0][0:2]
        if solo_hora=="22":
            cantidad=150
            bot_send_text("1202513315",f'Facundo se activo el remanente de las 22 del bot para mandar {cantidad} hsm')
            
            alentar_pedidos(sacarcobertura = "SI", hsm_por_hilo = 15, cant_hilos = 10)
        if solo_hora=="23":
            cantidad=20
            bot_send_text("1202513315",f'Facundo se activo el remanente de las 23 del bot para mandar {cantidad} hsm')
            alentar_pedidos(sacarcobertura = "SI", hsm_por_hilo = 2, cant_hilos = 10)
    
    else:
        print("Se activó el bot pero no cumple con condiciones.")
        bot_send_text("1202513315",f'Facundo se activo el bot para mandar hsm pero no corrió por no cumplir con ninguna condición')

# Bucle mmatos 

# if __name__=="__main__":    
#     flag = False
#     while flag == False:
#         try:
#             dia_actual = hoy()
#             feriados = feriados()

#             #procesar_pedidos('QAS', dia_actual,feriados)
#             procesar_pedidos('PRD', dia_actual,feriados)
            
#         except Exception as exc:
#             print(f'Pincho mmatos. Aviso por mail y rompo bucle')
#             service = main()
#             enviarMensaje(service)
#             flag = True
