use crate::datastore::DataStore;
use crate::schema::{ColumnDef, DataType, Schema};
use std::collections::HashMap;

pub struct Catalog {
    tables: HashMap<String, DataStore>,
}

impl Catalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> bool {
        let key = name.to_uppercase();
        if self.tables.contains_key(&key) {
            return false;
        }
        self.tables.insert(key, DataStore::new(schema));
        true
    }

    pub fn create_table_if_not_exists(&mut self, name: &str, schema: Schema) -> bool {
        let key = name.to_uppercase();
        if !self.tables.contains_key(&key) {
            self.tables.insert(key, DataStore::new(schema));
            return true;
        }
        false
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(&name.to_uppercase()).is_some()
    }

    pub fn get_table(&self, name: &str) -> Option<&DataStore> {
        self.tables.get(&name.to_uppercase())
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut DataStore> {
        self.tables.get_mut(&name.to_uppercase())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_uppercase())
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}
