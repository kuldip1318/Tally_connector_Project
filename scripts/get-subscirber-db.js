var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
function createOrUpdateCompanyDetailsTable(client) {
    return __awaiter(this, void 0, void 0, function () {
        var primaryKeyColumn, columns, _i, columns_1, column;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, tableExists(client, 'company_details')];
                case 1:
                    if (!_a.sent()) return [3 /*break*/, 15];
                    console.log('company_details table already exists. Checking structure...');
                    return [4 /*yield*/, getPrimaryKeyColumn(client, 'company_details')];
                case 2:
                    primaryKeyColumn = _a.sent();
                    if (!!primaryKeyColumn) return [3 /*break*/, 8];
                    return [4 /*yield*/, columnExists(client, 'company_details', 'company_id')];
                case 3:
                    if (!!(_a.sent())) return [3 /*break*/, 5];
                    return [4 /*yield*/, client.query("\n            ALTER TABLE company_details\n            ADD COLUMN company_id SERIAL PRIMARY KEY;\n          ")];
                case 4:
                    _a.sent();
                    console.log('Added company_id column as primary key to company_details table.');
                    return [3 /*break*/, 7];
                case 5: return [4 /*yield*/, client.query("\n            ALTER TABLE company_details\n            ADD PRIMARY KEY (company_id);\n          ")];
                case 6:
                    _a.sent();
                    console.log('Set company_id as primary key for company_details table.');
                    _a.label = 7;
                case 7: return [3 /*break*/, 9];
                case 8:
                    console.log("Primary key (".concat(primaryKeyColumn, ") already exists for company_details table."));
                    _a.label = 9;
                case 9:
                    columns = ['business_name', 'gst_number', 'created_at'];
                    _i = 0, columns_1 = columns;
                    _a.label = 10;
                case 10:
                    if (!(_i < columns_1.length)) return [3 /*break*/, 14];
                    column = columns_1[_i];
                    return [4 /*yield*/, columnExists(client, 'company_details', column)];
                case 11:
                    if (!!(_a.sent())) return [3 /*break*/, 13];
                    return [4 /*yield*/, client.query("\n            ALTER TABLE company_details\n            ADD COLUMN ".concat(column, " ").concat(column === 'created_at' ? 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' : 'VARCHAR(255)', ";\n          "))];
                case 12:
                    _a.sent();
                    console.log("Added ".concat(column, " column to company_details table."));
                    _a.label = 13;
                case 13:
                    _i++;
                    return [3 /*break*/, 10];
                case 14: return [3 /*break*/, 17];
                case 15: return [4 /*yield*/, client.query("\n        CREATE TABLE company_details (\n          company_id SERIAL PRIMARY KEY,\n          business_name VARCHAR(255) NOT NULL,\n          gst_number VARCHAR(255),\n          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n        )\n      ")];
                case 16:
                    _a.sent();
                    console.log('company_details table created successfully.');
                    _a.label = 17;
                case 17: return [2 /*return*/];
            }
        });
    });
}
function createOrUpdateSubscriberDbTable(client) {
    return __awaiter(this, void 0, void 0, function () {
        var usersPrimaryKey, companyPrimaryKey;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, tableExists(client, 'subscriber_db')];
                case 1:
                    if (!!(_a.sent())) return [3 /*break*/, 5];
                    return [4 /*yield*/, getPrimaryKeyColumn(client, 'users_tb')];
                case 2:
                    usersPrimaryKey = _a.sent();
                    return [4 /*yield*/, getPrimaryKeyColumn(client, 'company_details')];
                case 3:
                    companyPrimaryKey = _a.sent();
                    return [4 /*yield*/, client.query("\n        CREATE TABLE subscriber_db (\n          subscribe_id SERIAL PRIMARY KEY,\n          user_id INTEGER NOT NULL,\n          company_id INTEGER NOT NULL,\n          tally_company VARCHAR(255) NOT NULL,\n          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n          FOREIGN KEY (user_id) REFERENCES users_tb(".concat(usersPrimaryKey, "),\n          FOREIGN KEY (company_id) REFERENCES company_details(").concat(companyPrimaryKey, ")\n        )\n      "))];
                case 4:
                    _a.sent();
                    console.log('subscriber_db table created successfully.');
                    return [3 /*break*/, 6];
                case 5:
                    console.log('subscriber_db table already exists.');
                    _a.label = 6;
                case 6: return [2 /*return*/];
            }
        });
    });
}
function createOrUpdateOtherTables(client) {
    return __awaiter(this, void 0, void 0, function () {
        var otherTables, _i, otherTables_1, tableName, usersPrimaryKey, companyPrimaryKey, createTableQuery;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    otherTables = ['tally_data', 'tally_groups', 'tally_ledgers', 'ledger_monthly_summary'];
                    _i = 0, otherTables_1 = otherTables;
                    _a.label = 1;
                case 1:
                    if (!(_i < otherTables_1.length)) return [3 /*break*/, 8];
                    tableName = otherTables_1[_i];
                    return [4 /*yield*/, tableExists(client, tableName)];
                case 2:
                    if (!!(_a.sent())) return [3 /*break*/, 6];
                    return [4 /*yield*/, getPrimaryKeyColumn(client, 'users_tb')];
                case 3:
                    usersPrimaryKey = _a.sent();
                    return [4 /*yield*/, getPrimaryKeyColumn(client, 'company_details')];
                case 4:
                    companyPrimaryKey = _a.sent();
                    createTableQuery = "\n          CREATE TABLE ".concat(tableName, " (\n            id SERIAL PRIMARY KEY,\n            subscribe_id INTEGER REFERENCES subscriber_db(subscribe_id),\n            user_id INTEGER REFERENCES users_tb(").concat(usersPrimaryKey, "),\n            company_id INTEGER REFERENCES company_details(").concat(companyPrimaryKey, "),\n        ");
                    if (tableName === 'tally_data') {
                        createTableQuery += "\n            date DATE,\n            voucher_type TEXT,\n            voucher_number TEXT,\n            party_name TEXT,\n            voucher_category TEXT,\n            narration TEXT,\n            ledger TEXT,\n            amount NUMERIC\n          )";
                    }
                    else if (tableName === 'tally_groups') {
                        createTableQuery += "\n            GUID TEXT UNIQUE,\n            Name TEXT,\n            Parent TEXT,\n            PrimaryGroup TEXT,\n            Nature TEXT,\n            Sign TEXT,\n            Gross_Net_Profit TEXT,\n            SortPosition BIGINT\n          )";
                    }
                    else if (tableName === 'tally_ledgers') {
                        createTableQuery += "\n            GUID TEXT UNIQUE,\n            Name TEXT,\n            Parent TEXT,\n            Nature TEXT,\n            Sign TEXT,\n            SortPosition BIGINT\n          )";
                    }
                    else if (tableName === 'ledger_monthly_summary') {
                        createTableQuery += "\n            ledger TEXT,\n            financial_year TEXT,\n            month TEXT,\n            opening NUMERIC,\n            debit NUMERIC,\n            credit NUMERIC,\n            closing NUMERIC,\n            CONSTRAINT ".concat(tableName, "_unique_constraint \n            UNIQUE (subscribe_id, user_id, company_id, ledger, financial_year, month)\n          )");
                    }
                    return [4 /*yield*/, client.query(createTableQuery)];
                case 5:
                    _a.sent();
                    console.log("".concat(tableName, " table created successfully."));
                    return [3 /*break*/, 7];
                case 6:
                    console.log("".concat(tableName, " table already exists."));
                    _a.label = 7;
                case 7:
                    _i++;
                    return [3 /*break*/, 1];
                case 8: return [2 /*return*/];
            }
        });
    });
}
