/* ..........................................................................
**
**
**
** ..........................................................................
**/
package com.mycompany.system.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;

import com.mycompany.common.pool.PoolableObject;

public class DBConnection implements PoolableObject {
    protected Connection _conn = null;
    protected String _sid;
    protected boolean _profile = false;
    protected boolean _closed = false;
    
    private String _info;
    private long createdTime;
    private long lastUsed = 0;

	public DBConnection(Connection conn) {
		_conn = conn;
		
        createdTime = System.currentTimeMillis();
    }
	
	public DBConnection(Connection conn, boolean profile) {
		_conn = conn;
		_profile = profile;
		
		createdTime = System.currentTimeMillis();
	}
	
	/**
	 * info stores the JDBC info of this connection
	 * 
	 * @param info
	 */
	protected void setInfo (String info) {
		this._info = info;
	}
	
	/**
	 * getInfo returns JDBC info of this connection
	 * @return String
	 */
	public String getInfo () {
		return _info;
	}
	
	public Connection getConnection() {
		return _conn;
	}

	/**
	 * @return
	 */
	public String getSID() {
		return _sid;
	}

	/**
	 * @param string
	 */
	public void setSID(String sid) {
		this._sid = sid;
	}
	
	public void activate() {
        _closed = false;
        setLastUsed();
	}
	
	/**
	 * Doesn't close the underlying {@link Connection}.
	 */
	public void passivate() {
	    setLastUsed(0);
	    _closed = false;
    }
	
	public void checkOpen() throws DBException {
        if(_closed) {
            throw new DBException("Connection is closed.");
        }
    }
	
	public void commit() throws DBException
    { checkOpen(); try { _conn.commit(); } catch (SQLException e) { handleException(e); } }
	
	public void clearWarnings() throws DBException
    { checkOpen(); try { _conn.clearWarnings(); } catch (SQLException e) { handleException(e); } }
    
    public boolean getAutoCommit() throws DBException
    { checkOpen(); try { return _conn.getAutoCommit(); } catch (SQLException e) { handleException(e); return false; } 
    }
    public String getCatalog() throws DBException
    { checkOpen(); try { return _conn.getCatalog(); } catch (SQLException e) { handleException(e); return null; } }
    
    public DatabaseMetaData getMetaData() throws DBException
    { checkOpen(); try { return _conn.getMetaData(); } catch (SQLException e) { handleException(e); return null; } }
    
    public int getTransactionIsolation() throws DBException
    { checkOpen(); try { return _conn.getTransactionIsolation(); } catch (SQLException e) { handleException(e); return -1; } }
    
    public Map getTypeMap() throws DBException
    { checkOpen(); try { return _conn.getTypeMap(); } catch (SQLException e) { handleException(e); return null; } }
    
    public SQLWarning getWarnings() throws DBException
    { checkOpen(); try { return _conn.getWarnings(); } catch (SQLException e) { handleException(e); return null; } }
    
    public boolean isReadOnly() throws DBException
    { checkOpen(); try { return _conn.isReadOnly(); } catch (SQLException e) { handleException(e); return false; } }
    
    public String nativeSQL(String sql) throws DBException
    { checkOpen(); try { return _conn.nativeSQL(sql); } catch (SQLException e) { handleException(e); return null; } }
    
    public void rollback() throws DBException
    { checkOpen(); try {  _conn.rollback(); } catch (SQLException e) { handleException(e); } }
    
    public void setAutoCommit(boolean autoCommit) throws DBException
    { checkOpen(); try { _conn.setAutoCommit(autoCommit); } catch (SQLException e) { handleException(e); } }

    public void setCatalog(String catalog) throws DBException
    { checkOpen(); try { _conn.setCatalog(catalog); } catch (SQLException e) { handleException(e); } }

    public void setReadOnly(boolean readOnly) throws DBException
    { checkOpen(); try { _conn.setReadOnly(readOnly); } catch (SQLException e) { handleException(e); } }

    public void setTransactionIsolation(int level) throws DBException
    { checkOpen(); try { _conn.setTransactionIsolation(level); } catch (SQLException e) { handleException(e); } }

    public void setTypeMap(Map map) throws DBException
    { checkOpen(); try { _conn.setTypeMap(map); } catch (SQLException e) { handleException(e); } }
	
	public boolean isClosed() throws DBException {
		try {
	        if(_closed || _conn.isClosed()) {
	            return true;
	        }
		} catch (SQLException e) {}
        
		return false;
   }
	
	public boolean equals(Object obj) {
        if (_conn == null) {
            return false;
        }

        return _conn.equals(obj);
    }

    public int hashCode() {
        Object obj = _conn;
        if (obj == null) {
            return 0;
        }
        return obj.hashCode();
    }
	
    protected void handleException(SQLException e) throws DBException {
        throw new DBException(e);
    }
     
     /**
      * Actually close the underlying {@link Connection}.
      */
     public void destroy() throws DBException {
     	passivate();
     	try {
     		_conn.close();
     	} catch (SQLException e) {
     		throw new DBException(e);
     	}
     }
     
     /**
      * Set the time this object was last used to the
      * current time in ms.
      */
     public void setLastUsed() {
     	lastUsed = System.currentTimeMillis();
     }
     
     /**
      * Set the time in ms this object was last used.
      *
      * @param long time in ms
      */
     public void setLastUsed(long time) {
         lastUsed = time;
     }
     
     public String toString() {
         return _info;
     }
     
     public long getCreatedTime() {
    	 return createdTime;
     }
     
     public long getLastUsed() {
    	 return lastUsed;
     }
}
