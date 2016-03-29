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
public class Prueba3 {
    /**
     * Main del proyecto.
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Prueba conexión MongoDB");
        MongoClient mongo = crearConexion();
 
        if (mongo != null) {
 
            //Si no existe la base de datos la crea
            DB db = mongo.getDB("bd-ejemplo");
            System.out.println();
 
            //Crea una tabla si no existe y agrega datos
            insertTrabajador(db, "trabajador", "Jose", "Lopez Perez", 45);
            insertTrabajador(db, "trabajador", "Maria", "Martinez Aguilar", 35);
            insertTrabajador(db, "trabajador", "Juan", "Navarro Robles", 25);
            insertTrabajador(db, "trabajador", "Lucia", "Casas Meca", 66);
            insertTrabajador(db, "trabajador", "Jose", "Naranjo Moreno", 33);
            insertTrabajador(db, "trabajador", "Jose Luis", "Romero Diaz", 55);
            insertTrabajador(db, "trabajador", "Ana", "Canovas Perez", 24);
            insertTrabajador(db, "trabajador", "Lucia", "Martinez Martinez", 67);
 
            //Actualiza la edad de los trabajadores con el nombre "Jose"
            updateNombreTrabajador(db, "trabajador", "Jose", 46);
 
            //Listar la tabla "trabajador"
            System.out.println("Listar los registros de la tabla: ");
            selectTablas(db, "trabajador");
            System.out.println();
 
            //Listar de la tabla "trabajador" aquellos que se llamen "Jose"
            System.out.println("Listar los registros de la tabla cuyo nombre sea Jose: ");
            selectTablasWhere(db, "trabajador", "Jose");
            System.out.println();
 
            //Eliminar los trabajadores cuyo nombre sean "Ana"
            deleteTrabajadorPorNombre(db, "trabajador", "Ana");
 
            //Eliminar los trabajadores cuyos anyos sean mayor que 50
            deleteTrabajadorEdadMayorQue(db, "trabajador", 65);
 
            //Eliminar los trabajadores con los apellidos abajo indicados en la lista
            List lista = new ArrayList();
            lista.add("Casas Meca");
            lista.add("Navarro Robles");
            deleteTrabajadorEnLista(db, "trabajador", lista);
 
            //Listar la tabla "trabajador" (otra vez)
            System.out.println("Listar los registros de la tabla: ");
            selectTablas(db, "trabajador");
            System.out.println();
 
            //Listar las tablas de la base de datos actual
            System.out.println("Lista de tablas de la base de datos: ");
            printColecciones(db);
            System.out.println();
 
            //Listas las bases de datos
            System.out.println("Lista de todas las bases de datos: ");
            printDatabases(mongo);
            System.out.println();
 
            //Borrar base de datos
            //db.dropDatabase();
 
        } else {
            System.out.println("Error: Conexión no establecida");
        }
    }
 
    /**
     * Ejemplo para crear una conexión a MongoDB.
     * @return MongoClient conexión
     */
    private static MongoClient crearConexion() {
        MongoClient mongo = null;
        try {
            mongo = new MongoClient("localhost", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return mongo;
    }
 
    /**
     * Ejemplo que imprime por pantalla todas las bases de datos MongoDB.
     * @param mongo conexión a MongoDB
     */
    private static void printDatabases(MongoClient mongo) {
        List basesDeDatos = mongo.getDatabaseNames();
        for (Object nombreBaseDatos : basesDeDatos) {
            System.out.println(" - " + nombreBaseDatos);
        }
    }
 
    /**
     * Ejemplo que imprime por pantalla todas las colecciones / tablas de una base de datos.
     * @param db de tipo DB
     */
    private static void printColecciones(DB db) {
        Set tables = db.getCollectionNames();
 
        for(Object coleccion : tables){
            System.out.println(" - " + coleccion);
        }
    }
 
    /**
     * Ejemplo que inserta un registro dado un DB, nombre de tabla y campos de la tabla (id, nombre, apellidos y años).
     * @param db
     * @param tabla
     * @param id
     * @param nombre
     * @param apellidos
     * @param anyos
     */
    private static void insertTrabajador(DB db, String tabla, String nombre, String apellidos, int anyos) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Crea un objecto básico y le añade elementos
        BasicDBObject document = new BasicDBObject();
        document.put("nombre", nombre);
        document.put("apellidos", apellidos);
        document.put("anyos", anyos);
        document.put("fecha", new Date());
 
        //Inserta la tabla
        table.insert(document);
    }
 
    /**
     * Ejemplo que modifica el campo anyos dado una DB, tabla e id de usuario.
     * Hay varias formas de actualizar los registros, y ésta es una de ella.
     * Otros tipos de actualización de registros aquí: http://www.mkyong.com/mongodb/java-mongodb-update-document/
     *
     * @param db
     * @param tabla
     * @param id
     * @param nuevosAnyos
     */
    private static void updateNombreTrabajador(DB db, String tabla, String nombre, int nuevosAnyos) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Prepara para insertar un nuevo campo
        BasicDBObject updateAnyos = new BasicDBObject();
        updateAnyos.append("$set", new BasicDBObject().append("anyos", nuevosAnyos));
 
        //Busca el/los registro/s con el nombre indicado
        BasicDBObject searchById = new BasicDBObject();
        searchById.append("nombre", nombre);
 
        //Realiza la actualización
        table.updateMulti(searchById, updateAnyos);
    }
 
    /**
     * Ejemplo que imprime por pantalla todos los trabajadores
     * @param db
     * @param tabla
     */
    private static void selectTablas(DB db, String tabla) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Busca y muestra todos los datos de la tabla
        DBCursor cur = table.find();
        while (cur.hasNext()) {
            System.out.println(" - " + cur.next().get("nombre") + " " + cur.curr().get("apellidos") + " -- " + cur.curr().get("anyos") + " años (" + cur.curr().get("fecha") + ")");
        }
    }
 
    /**
     * Ejemplo que imprime por pantalla todos los trabajadores con nombre indicado
     * @param db
     * @param tabla
     * @param nombre
     */
    private static void selectTablasWhere(DB db, String tabla, String nombre) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Creando el filtro en el campo "nombre"
        BasicDBObject query = new BasicDBObject();
        query.put("nombre", nombre);
 
        //Busca y muestra todos los datos de la tabla donde "nombre" sea el indicado
        DBCursor cur = table.find(query);
        while (cur.hasNext()) {
            System.out.println(" - " + cur.next().get("nombre") + " " + cur.curr().get("apellidos") + " -- " + cur.curr().get("anyos") + " años (" + cur.curr().get("fecha") + ")");
        }
    }
 
    /**
     * Ejemplo que elimina los trabajadores con nombre indicado
     * @param db
     * @param tabla
     * @param nombre
     */
    private static void deleteTrabajadorPorNombre(DB db, String tabla, String nombre) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Indica el campo y valor y lo elimina
        table.remove(new BasicDBObject().append("nombre", nombre));
    }
 
    /**
     * Ejemplo que elimina los trabajadores con una edad mayor a la indicada
     * @param db
     * @param tabla
     * @param anyos
     */
    private static void deleteTrabajadorEdadMayorQue(DB db, String tabla, int anyos) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Indica el campo y valor que ha de ser mayor para eliminarlo
        BasicDBObject query = new BasicDBObject();
        query.put("anyos", new BasicDBObject("$gt", anyos));
        table.remove(query);
    }
 
    /**
     * Ejemplo que elimina los trabajadores cuyos apellidos estén en una lista
     * @param db
     * @param tabla
     * @param lista
     */
    private static void deleteTrabajadorEnLista(DB db, String tabla, List lista) {
        //Recoge datos de la tabla
        DBCollection table = db.getCollection(tabla);
 
        //Indica la lista de nombres que quiere eliminar
        BasicDBObject query = new BasicDBObject();
        query.put("apellidos", new BasicDBObject("$in", lista));
        table.remove(query);
    }
}