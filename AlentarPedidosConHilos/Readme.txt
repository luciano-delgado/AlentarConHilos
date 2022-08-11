
    - Quitar pedidos que tengas % AFI >0: Eliminado 30/3, vuelto a poner 11.04

20/07:
    Detalle de hilos:
    1) El df15 (original) se explota en la cant de hilos ingresada como parámetro a la función alentar
    2) Los hilos se crean a partir de una clase para poder recuperar el atributo result (cantidaddepedidoscoordinados)
    3) Se agrega dentro del bucle por cada afiliado la condicion de cantidaddepedidoscoordinados < hsm_max_por_hilo para que sea evaluada a nivel Pedidos.
    4) Queda un gris cuando las posiciones del df15 inicial no dan un numero redondo al dividir por la cant de hilos
21/07:
    - Punto 4) del 20/07 solucionado al ajustar los intervalos que toma cada df por hilo. 

Para pasar a PRD:
    - Cambiar usuario Hana
    - Descomentar funciones:
        bot_send_text
        comunicar_alentar_pedido_para_coordinar
        cambiar_bloqueos_por_api_PRD(pedido,fentrerealfinal,destinatario,"WP")
        cambiar_bloqueos_por_api_PRD(pedido,fentrerealfinal,destinatario,"WA")

22/7: Se pasa a PRD.