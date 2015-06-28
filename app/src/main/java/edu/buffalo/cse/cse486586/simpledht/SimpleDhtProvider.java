package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    //String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1 , REMOTE_PORT2};
    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1,REMOTE_PORT2 ,REMOTE_PORT3,REMOTE_PORT4};
    int totalAliveNodes = 0;
    boolean resultReceived = false;
    static String myPort;
    static String myPortForHash;
    String successorPort;
    String predecessorPort;
    String queryGlobalSender = null;
    String queryGlobalResult = null;
    String deleteGlobalSender = null;
    String firstChordNode = null;
    boolean firstInRing = false;
    //NodeIdInChord , Port
    HashMap<String,String> activeAvds = new HashMap<String,String>();

    String queryResult = null;
    String querySender = null;

    String deleteSender = null;

    String[] columnNames = {"key","value"};
    MatrixCursor globalCursor;
    boolean queryGlobalResponseReceived = false;

    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("\"@\"")) {
            FileInputStream fis;
            Log.v("delete", " Deleting local: all key:value pairs");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledht/files");
            File[] files = dir.listFiles();

            if (files != null) {
                for (int i = 0; i < files.length; i++) {

                    files[i].delete();

                }
            }
        }
        else if(selection.equals("\"*\""))
        {
            FileInputStream fis;
            Log.v("delete", " Deleting local: all key:value pairs");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledht/files");
            File[] files = dir.listFiles();

            if (files != null) {
                for (int i = 0; i < files.length; i++) {

                    files[i].delete();

                }
            }
            if(!(successorPort.equals(myPort) && predecessorPort.equals(myPort))) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "deleteGlobal");
            }


        }
        else
        {
            FileInputStream fis;
            Log.v("delete", " Deleting key:value pair");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledht/files");
            File[] files = dir.listFiles();
            boolean deleted = false;
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].getName().equals(selection)) {
                        files[i].delete();
                        deleted = true;
                    }
                }
            }

            if(deleted == false && deleteSender == null)
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "delete" , selection );

            }
            else if(deleted == false && deleteSender!=null)
            {
                if(!deleteSender.equals(myPort))
                {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteSender, successorPort, "delete" , selection );
                    deleteSender = null;
                }
                else
                {
                    deleteSender = null;
                }
            }

        }


        return 0;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key  = (String)values.get("key");
        String value = (String)values.get("value");

        String predecessorPortId = Integer.toString(Integer.parseInt(predecessorPort)/2);
        Log.v("insert" , "Predecessor : " + predecessorPortId + " MyPort:" + myPortForHash  + " Successor: " + successorPort);
        try
        {
            if(successorPort.equals(myPort) && predecessorPort.equals(myPort)) {
                Log.v("insert", key + " belongs to me : " + myPort);
                return insertKeyValue(uri, values);
            }
            else
            {

                Log.v("insert" , " Predecessor : " + predecessorPortId +   "Key:" +  key +
                        " MyPort:" + myPortForHash + " Compare pred with key:" +
                        genHash(predecessorPortId).compareTo(genHash(key)) + " Compare key with self:" + genHash(key).compareTo(genHash(myPortForHash)));
                // if first in ring , only one condition will be true depending on where the hash key value lies

                if(firstInRing)
                {
                    if (genHash(predecessorPortId).compareTo(genHash(key)) < 0 || genHash(key).compareTo(genHash(myPortForHash)) <= 0) {
                        Log.v("insert", key + "Inside FIRSTINRING: belongs to me : " + myPort);
                        return insertKeyValue(uri, values);
                    }
                    else {
                        Log.v("insert", key + " does not belong to me : " + myPort);
                        Log.v("insert", "Inside FIRSTINRING: Forwarding to successor" + successorPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "insert", key, value);
                    }
                }
                else {
                    if (genHash(predecessorPortId).compareTo(genHash(key)) < 0 && genHash(key).compareTo(genHash(myPortForHash)) <= 0) {
                        Log.v("insert", key + " belongs to me : " + myPort);
                        return insertKeyValue(uri, values);
                    } else {
                        Log.v("insert", key + " does not belong to me : " + myPort);
                        Log.v("insert", "Forwarding to successor" + successorPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "insert", key, value);
                    }
                }
            }
        }
        catch(Exception ex)
        {
            Log.e(TAG,"Insert - Error while hashing");
        }
        return  null;

    }

    public Uri insertKeyValue(Uri uri, ContentValues values)
    {
        String key  = (String)values.get("key");
        String value = (String)values.get("value");
        String filename = key;
        FileOutputStream outputStream;

        try {
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();


        } catch (Exception e)
        {
            Log.e("insert", "File write failed");
        }
        Log.v("insert", myPort + "- "+ key + " : " +  value );
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("AVDINFO","My port is " + myPort);
        myPortForHash = portStr;
        successorPort = myPort;
        predecessorPort = myPort;

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
        }
        //If the port of the current AVD is not 11108 (5554) , send a join request to avd with port 5554
        if(!myPort.equals("11108"))
        {
            Log.v("JOIN" , "Sending a join operation to 11108 from " + myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort , Integer.toString(11108), "join"  );
        }
        else
        {
            try {
                synchronized (activeAvds) {
                    activeAvds.put(genHash(myPortForHash), myPort);
                }
            }
            catch(Exception ex)
            {
                Log.e("SHA" , "Error while deriving node id on the chord");
            }
        }

        return false;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {

        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            while(true) {

                ServerSocket serverSocket = sockets[0];
                try {
                    Socket clientSocket = serverSocket.accept();

                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
                    String message = null;
                    String val;
                    while ((val = in.readLine()) != null) {
                        if (message == null)
                            message = val;
                        else
                            message = message + val;
                    }
                    MessageObject action = processMessage(message);
                    if(action.getOperation().equals("join") )
                    {
                        Log.v("JOIN" , "Received a join operation from " + action.getSender());
                        modifyAndUpdateChordRing(action.getSender());
                    }
                    else if(action.getOperation().equals("firstInRing"))
                    {
                        Log.v("JOIN" , "Received a first In Ring notification from " + action.getSender());
                        firstInRing = true;

                    }
                    else if(action.getOperation().equals("removeFirstInRing"))
                    {
                        Log.v("JOIN" , "Received a removeFirstInRing notification from " + action.getSender());
                        firstInRing = false;

                    }

                    else if(action.getOperation().equals("changeNotification"))
                    {
                        Log.v("NOTIFICATION" , "Received ring change notification from " + action.getSender());
                        if(!action.getPredecessor().equals("same"))
                            predecessorPort = action.getPredecessor();
                        if(!action.getSuccessor().equals("same"))
                            successorPort = action.getSuccessor();
                        Log.v("NOTIFICATION", "I am : " + myPort + " Updated Predecessor: " + predecessorPort + " Successor: " + successorPort);

                    }
                    else if(action.getOperation().equals("insert"))
                    {
                        Log.v("insert","Received insert request from " + action.getSender() + " for " + action.getKey());
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        String key = action.getKey();
                        String value = action.getValue();
                        ContentValues cv = new ContentValues();
                        cv.put("key",key);
                        cv.put("value", value);
                        insert(mUri,cv);

                    }
                    else if( action.getOperation().equals("query"))
                    {
                        synchronized (this) {

                            Log.v("query", "Received query request from " + action.getSender() + " for " + action.getKey());
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            querySender = action.getSender();

                            Cursor resultCursor = query(mUri, null, action.getKey(),
                                    null, null);

                            if (resultCursor != null && resultCursor.getCount() > 0) {
                                int keyIndex = resultCursor.getColumnIndex("key");

                                int valueIndex = resultCursor.getColumnIndex("value");
                                resultCursor.moveToFirst();

                                String key = resultCursor.getString(keyIndex);

                                String value = resultCursor.getString(valueIndex);

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, action.getSender(), "queryResult", action.getKey(), value);

                            }
                        }

                    }
                    else if(action.getOperation().equals("queryResult"))
                    {
                        Log.v("query", "Received query result from " + action.getSender() + " for " + action.getKey() +" as" + action.getValue());
                        queryResult = action.getValue();

                    }
                    else if(action.getOperation().equals("queryGlobal"))
                    {

                        Log.v("queryglobal", "Received queryglobal request from " + action.getSender());
                        queryGlobalSender = action.getSender();

                        if(!queryGlobalSender.equals(myPort)) {
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                            Cursor resultCursor = query(mUri, null, "\"@\"",
                                    null, null);
                            String result = null;
                            Log.v("queryglobal", "Checking " + resultCursor.getCount());
                            resultCursor.moveToPosition(-1);
                            while (resultCursor.moveToNext()) {
                                int keyIndex = resultCursor.getColumnIndex("key");
                                int valueIndex = resultCursor.getColumnIndex("value");
                                String key = resultCursor.getString(keyIndex);
                                String value = resultCursor.getString(valueIndex);
                                Log.v("queryglobal", "Checking " + key + ":" + value);


                                if (result == null)
                                    result = key + ":" + value;
                                else
                                    result += "-" + key + ":" + value;

                            }
                            if (result == null )
                                result = "";

                            if(!action.getResult().equals("start"))
                            {
                                if(result.equals(""))
                                    result = action.getResult();
                                else
                                    result = result + "-" + action.getResult();
                            }
                            else
                            {
                                if(result.equals(""))
                                    result = "start";
                            }

                            Log.v("queryglobal", result);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryGlobalSender, successorPort, "queryGlobal", result);
                        }
                        else
                        {
                            queryGlobalResult = action.getResult();
                            queryGlobalResponseReceived = true;

                        }

                    }
                    else if(action.getOperation().equals("deleteGlobal"))
                    {
                        deleteGlobalSender = action.getSender();
                        if(!deleteGlobalSender.equals(myPort))
                        {
                            Log.v("deleteglobal", "Received deleteglobal request from " + action.getSender());
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                            delete(mUri, "\"@\"", null);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteGlobalSender, successorPort, "deleteGlobal");


                        }
                        else
                        {
                            deleteGlobalSender = null;
                        }
                    }
                    else if(action.getOperation().equals("delete"))
                    {
                        synchronized (this) {
                            Log.v("delete", "Received delete request from " + action.getSender());
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            deleteSender = action.getSender();

                            delete(mUri, action.getKey(), null);
                        }
                    }



                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                    Log.e(TAG, "IO Exception while receiving message " + ex.getMessage() );
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                    Log.e(TAG, "Exception while receiving message " + ex.getMessage()  );
                }
            }
        }


        public synchronized MessageObject processMessage(String message) {
            String[] moInStr = message.split("#");
            MessageObject mo = new MessageObject();
            mo.setSender(moInStr[0]);
            mo.setReceiver(moInStr[1]);
            mo.setOperation(moInStr[2]);
            if(moInStr.length > 3 && mo.getOperation().equals("changeNotification"))
            {

                mo.setPredecessor(moInStr[3]);
                mo.setSuccessor(moInStr[4]);
            }
            if(moInStr.length > 3 && mo.getOperation().equals("insert"))
            {
                mo.setKey(moInStr[3]);
                mo.setValue(moInStr[4]);
            }

            if(moInStr.length > 3 && mo.getOperation().equals("query"))
            {
                mo.setKey(moInStr[3]);
            }

            if(moInStr.length > 3 && mo.getOperation().equals("queryResult"))
            {
                mo.setKey(moInStr[3]);
                mo.setValue(moInStr[4]);
            }


            if(moInStr.length > 3 && mo.getOperation().equals("delete"))
            {
                mo.setKey(moInStr[3]);
            }

            if(moInStr.length > 3 && mo.getOperation().equals("queryGlobal"))
            {
                mo.setResult(moInStr[3]);
            }



            return mo;
        }


        public synchronized void modifyAndUpdateChordRing(String joiningNodePort)
        {

            Log.v("JOIN" , "Handling Join Operation for " + joiningNodePort);
            String joiningNodeChordId = null;

            try {
                joiningNodeChordId = genHash(Integer.toString(Integer.parseInt(joiningNodePort)/2));
                synchronized (activeAvds) {
                    activeAvds.put(joiningNodeChordId, joiningNodePort);
                }
            }
            catch(Exception ex)
            {
                Log.e("SHA" , "Error while deriving node id on the chord");
            }

            //Organize all nodes in a ring after addition of new node in the chord ring
            List<String> nodeIDsinChord = new ArrayList<>(activeAvds.keySet());
            Collections.sort(nodeIDsinChord); // Sort by hashed port
            String joiningNodePre;
            String joiningNodeSucc;
            Log.v("SHA", "First :" + activeAvds.get(nodeIDsinChord.get(0)));
            String first = activeAvds.get(nodeIDsinChord.get(0));
            if(firstChordNode != null)
            {
                if(!firstChordNode.equals(first))
                {
                    Log.v("JOIN" , "Changing first in ring to " + first + " from "+firstChordNode );
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, firstChordNode, "removeFirstInRing");

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, first, "firstInRing");
                    firstChordNode = first;
                }
            }
            else {
                firstChordNode = first;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinChord.get(0)), "firstInRing");
            }
            for(int i = 0; i < nodeIDsinChord.size() ; i++)
            {
                if(nodeIDsinChord.get(i).equals(joiningNodeChordId))
                {
                        //If new node gets inserted at the 0th position
                        if( i == 0)
                        {
                            joiningNodePre = activeAvds.get(nodeIDsinChord.get(nodeIDsinChord.size() - 1));
                            joiningNodeSucc = activeAvds.get(nodeIDsinChord.get(i + 1));
                        }
                        //If new node inserted at last position
                        else if( i == nodeIDsinChord.size() - 1)
                        {
                            joiningNodeSucc = activeAvds.get(nodeIDsinChord.get(0));
                            joiningNodePre = activeAvds.get(nodeIDsinChord.get( i - 1));
                        }
                        //If new node is inserted in between
                        else
                        {
                            joiningNodePre = activeAvds.get(nodeIDsinChord.get(i-1));
                            joiningNodeSucc = activeAvds.get(nodeIDsinChord.get(i + 1));
                        }
                        //Modify Affected Nodes
                        if(nodeIDsinChord.size() == 2)
                        {
                            //Notify joining Node for changes
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , myPort, joiningNodePort,"changeNotification", joiningNodePre,joiningNodeSucc );

                            //Notify Other Node for changes
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , myPort, joiningNodePre,"changeNotification", joiningNodePort,joiningNodePort );

                        }
                        else
                        {
                            //Notify the Joining Node for changes
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, joiningNodePort, "changeNotification", joiningNodePre, joiningNodeSucc);

                            //Notify the joining node's predecessor for changes
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, joiningNodePre, "changeNotification", "same", joiningNodePort);

                            //Notify the joining node's successor for changes
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, joiningNodeSucc, "changeNotification", joiningNodePort, "same");
                        }

                }
            }

        }



    }

    private class ClientTask extends AsyncTask<String, Void, Void>
    {

        @Override
        protected Void doInBackground(String... msgs)
        {
            if(msgs.length == 3) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation
                    out.println(msgs[0] + "#" + msgs[1] + "#" + msgs[2]);
                    Log.v(msgs[2].toUpperCase() , "Sending a " + msgs[2] + " operation to " + msgs[1] + " from " + msgs[0] );
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("changeNotification") && msgs.length == 5)
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#pre#succ
                    out.println(msgs[0]+"#"+msgs[1]+"#changeNotification#" +msgs[3]+"#"+msgs[4]);
                    Log.v("NOTIFICATION", "Sending a ring change notification message to " + msgs[1] + " from " + msgs[0]);
                    Log.v("NOTIFICATION", "Sending a ring change notification message to " + msgs[1] + " to change pred to :" + msgs[3] + " and succ to " + msgs[4] );
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("insert") && msgs.length == 5)
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#key#value
                    out.println(msgs[0]+"#"+msgs[1]+"#insert#" +msgs[3]+"#"+msgs[4]);
                    Log.v("insert", "Sending a insert message to " + msgs[1] + " from " + msgs[0]);
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("query"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#key#value
                    out.println(msgs[0]+"#"+msgs[1]+"#query#" +msgs[3]);
                    Log.v("query", "Sending a query message to " + msgs[1] + " from " + msgs[0]);
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("queryResult"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#key#value
                    out.println(msgs[0]+"#"+msgs[1]+"#queryResult#" +msgs[3]+"#" + msgs[4]);
                    Log.v("query", "Sending a query result  to " + msgs[1] + " from " + msgs[0]);
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("delete"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#key
                    out.println(msgs[0]+"#"+msgs[1]+"#delete#" +msgs[3]);
                    Log.v("query", "Sending a delete request to " + msgs[1] + " from " + msgs[0]);
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("queryGlobal"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#result
                    out.println(msgs[0]+"#"+msgs[1]+"#queryGlobal#" +msgs[3]);
                    Log.v("query", "Sending a queryGlobal request to " + msgs[1] + " from " + msgs[0]);
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG, e.getMessage());
                }
            }

            return null;
        }
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                                     String sortOrder) {
        // TODO Auto-generated method stub
        FileInputStream fis ;
        int awaitingResponses = 0 ;
        if(selection.equals("\"*\""))
        {
            globalCursor = new MatrixCursor(columnNames,0);
            Log.v("query", " Retrieving global key:value pairs");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledht/files");
            File[] files = dir.listFiles();

            if (files != null) {
                Log.v("query" ,"Files Found");
                for (int i = 0; i < files.length; i++) {
                    globalCursor.addRow(new String[]{files[i].getName(), readFile(files[i].getName())});
                }
            }

            if(!(successorPort.equals(myPort) && predecessorPort.equals(myPort))) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "queryGlobal", "start");

                Log.v("query" , "I am not the only one who knocks");
                while (!queryGlobalResponseReceived) {
                }
                Log.v("query", "Query Global Responses received ");
                queryGlobalResponseReceived = false;
                queryGlobalSender = null;
                if(queryGlobalResult!=null && !queryGlobalResult.equals("start")) {
                    if (queryGlobalResult.length() != 0) {
                        String[] gResult = queryGlobalResult.split("-");

                        for (int i = 0; i < gResult.length; i++) {

                            globalCursor.addRow(new String[]{gResult[i].split(":")[0], gResult[i].split(":")[1]});
                        }
                    }
                    queryGlobalResult = null;
                }
                    globalCursor.moveToPosition(-1);
                    while (globalCursor.moveToNext()) {
                        int keyIndex = globalCursor.getColumnIndex("key");
                        int valueIndex = globalCursor.getColumnIndex("value");
                        String key = globalCursor.getString(keyIndex);
                        String value = globalCursor.getString(valueIndex);
                        Log.v("query", "< " + key + " : " + value + " > ");
                    }

                //globalCursor.close();
            }
            return  globalCursor;

        }
        else if(selection.equals("\"@\""))
        {
            MatrixCursor cursor = new MatrixCursor(columnNames,0);

            Log.v("query" , " Retrieving local key:value pairs");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledht/files");
            File[] files = dir.listFiles();
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    cursor.addRow(new String[]{files[i].getName(), readFile(files[i].getName())});
                }
                //cursor.close();
            }
            while (cursor.moveToNext()) {
                int keyIndex = cursor.getColumnIndex("key");
                int valueIndex = cursor.getColumnIndex("value");
                String key = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);
                Log.v("query", "< " + key + " : " +  value + " > ") ;
            }
            return  cursor;
        }
        else {
            MatrixCursor cursor = new MatrixCursor(columnNames,0);

            String val = readFile(selection);
            if(val != null) {
                cursor.addRow(new String[]{selection, val});
                Log.v("query", selection + ":" + cursor.getCount());

                //cursor.close();
                queryResult = null;
                querySender = null;
                while (cursor.moveToNext()) {
                    int keyIndex = cursor.getColumnIndex("key");
                    int valueIndex = cursor.getColumnIndex("value");
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);
                    Log.v("query", "< " + key + " : " +  value + " > ") ;
                }
                return  cursor;

            }
            else
            {
                if (querySender == null) {
                    Log.v("query", "Forwarded "+ selection);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, successorPort, "query", selection);
                    while (queryResult == null) {
                        //Waiting for the query result to be received
                    }
                    Log.v("query", "Received Result "+ selection +":" +  queryResult );

                    cursor.addRow(new String[]{selection, queryResult});
                    //cursor.close();

                    queryResult = null;
                    querySender = null;

                } else {
                    Log.v("query" , "Forwarded" + selection + " : " + querySender );
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, querySender, successorPort, "query", selection);
                    queryResult = null;
                    querySender = null;
                }
                while (cursor.moveToNext()) {
                    int keyIndex = cursor.getColumnIndex("key");
                    int valueIndex = cursor.getColumnIndex("value");
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);
                    Log.v("query", "< " + key + " : " +  value + " > ") ;
                }
                return  cursor;

            }
        }
    }


    public String readFile(String key)
    {
        try {
            FileInputStream fis;
            String value = null;
            fis = getContext().openFileInput(key);
            if (fis != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String val;
                while ((val = br.readLine()) != null) {
                    if (value == null)
                        value = val;
                    else
                        value = value + val;
                }
            }

            return  value;
        }
        catch(Exception ex)
        {
            Log.e("query", "Error reading file with key " + key);
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
