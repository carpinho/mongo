package mongo.es;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
 
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
 
/**
 * Prueba para realizar conexión con MongoDB.
 * @author j
 *
 */
public class Prueba2 {
    /**
     * Main del proyecto.
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Prueba conexión MongoDB");
        MongoClient mongo = null;
        try {
            mongo = new MongoClient("localhost", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
 
        if (mongo != null) {
 
            //Si no existe la base de datos la crea
            DB db = mongo.getDB("bd-ejemplo");
 
            //Crea una tabla si no existe y agrega datos
            DBCollection table = db.getCollection("trabajador");
 
            //Crea los objectos básicos
            BasicDBObject document1 = new BasicDBObject();
            document1.put("nombre", "Jose");
            document1.put("apellidos", "Lopez Perez");
            document1.put("anyos", 45);
            document1.put("fecha", new Date());
 
            BasicDBObject document2 = new BasicDBObject();
            document2.put("nombre", "Maria");
            document2.put("apellidos", "Martinez Aguilar");
            document2.put("anyos", 35);
            document2.put("fecha", new Date());
 
            BasicDBObject document3 = new BasicDBObject();
            document3.put("nombre", "Juan");
            document3.put("apellidos", "Navarro Robles");
            document3.put("anyos", 25);
            document3.put("fecha", new Date());
 
            BasicDBObject document4 = new BasicDBObject();
            document4.put("nombre", "Lucia");
            document4.put("apellidos", "Casas Meca");
            document4.put("anyos", 66);
            document4.put("fecha", new Date());
 
            BasicDBObject document5 = new BasicDBObject();
            document5.put("nombre", "Jose");
            document5.put("apellidos", "Naranjo Moreno");
            document5.put("anyos", 33);
            document5.put("fecha", new Date());
 
            BasicDBObject document6 = new BasicDBObject();
            document6.put("nombre", "Jose Luis");
            document6.put("apellidos", "Romero Diaz");
            document6.put("anyos", 55);
            document6.put("fecha", new Date());
 
            BasicDBObject document7 = new BasicDBObject();
            document7.put("nombre", "Ana");
            document7.put("apellidos", "Canovas Perez");
            document7.put("anyos", 24);
            document7.put("fecha", new Date());
 
            BasicDBObject document8 = new BasicDBObject();
            document8.put("nombre", "Lucia");
            document8.put("apellidos", "Martinez Martinez");
            document8.put("anyos", 67);
            document8.put("fecha", new Date());
 
            //Insertar tablas
            table.insert(document1);
            table.insert(document2);
            table.insert(document3);
            table.insert(document4);
            table.insert(document5);
            table.insert(document6);
            table.insert(document7);
            table.insert(document8);
 
            //Actualiza la edad de los trabajadores con el nombre "Jose"
            BasicDBObject updateAnyos = new BasicDBObject();
            updateAnyos.append("$set", new BasicDBObject().append("anyos", 46));
 
            BasicDBObject searchById = new BasicDBObject();
            searchById.append("nombre", "Jose");
 
            table.updateMulti(searchById, updateAnyos);
 
            //Listar la tabla "trabajador"
            System.out.println("Listar los registros de la tabla: ");
            DBCursor cur = table.find();
            while (cur.hasNext()) {
                System.out.println(" - " + cur.next().get("nombre") + " " + cur.curr().get("apellidos") + " -- " + cur.curr().get("anyos") + " años (" + cur.curr().get("fecha") + ")");
            }
            System.out.println();
 
            //Listar de la tabla "trabajador" aquellos que se llamen "Jose"
            System.out.println("Listar los registros de la tabla cuyo nombre sea Jose: ");
            BasicDBObject query = new BasicDBObject();
            query.put("nombre", "Jose");
 
            DBCursor cur2 = table.find(query);
            while (cur2.hasNext()) {
                System.out.println(" - " + cur2.next().get("nombre") + " " + cur2.curr().get("apellidos") + " -- " + cur2.curr().get("anyos") + " años (" + cur2.curr().get("fecha") + ")");
            }
            System.out.println();
 
            //Eliminar los trabajadores cuyo nombre sean "Ana"
            table.remove(new BasicDBObject().append("nombre", "Ana"));
 
            //Eliminar los trabajadores cuyos anyos sean mayor que 50
            BasicDBObject query2 = new BasicDBObject();
            query2.put("anyos", new BasicDBObject("$gt", 65));
            table.remove(query2);
 
            //Eliminar los trabajadores con los apellidos abajo indicados en la lista
            List lista = new ArrayList();
            lista.add("Casas Meca");
            lista.add("Navarro Robles");
            BasicDBObject query3 = new BasicDBObject();
            query3.put("apellidos", new BasicDBObject("$in", lista));
            table.remove(query3);
 
            //Listar la tabla "trabajador" (otra vez)
            System.out.println("Listar los registros de la tabla: ");
            DBCursor cur3 = table.find();
            while (cur3.hasNext()) {
                System.out.println(" - " + cur3.next().get("nombre") + " " + cur3.curr().get("apellidos") + " -- " + cur3.curr().get("anyos") + " años (" + cur3.curr().get("fecha") + ")");
            }
            System.out.println();
 
            //Listar las tablas de la base de datos actual
            System.out.println("Lista de tablas de la base de datos: ");
            Set tables = db.getCollectionNames();
            for(Object coleccion : tables){
                System.out.println(" - " + coleccion);
            }
            System.out.println();
 
            //Listas las bases de datos
            System.out.println("Lista de todas las bases de datos: ");
            List basesDeDatos = mongo.getDatabaseNames();
            for (Object nombreBaseDatos : basesDeDatos) {
                System.out.println(" - " + nombreBaseDatos);
            }
            System.out.println();
 
            //Borrar base de datos
            db.dropDatabase();
 
        } else {
            System.out.println("Error: Conexión no establecida");
        }
    }
}