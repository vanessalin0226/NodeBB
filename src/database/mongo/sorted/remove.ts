import { MongoClient } from 'mongodb';
import helpers from '../helpers';

interface Module {
    client: MongoClient;
    sortedSetRemove: (key: string | string[], value: string | string[]) => Promise<void>;
    sortedSetsRemove: (keys: string[], value: string) => Promise<void>;
    sortedSetsRemoveRangeByScore: (keys: string[], min: string, max: string) => Promise<void>;
    sortedSetRemoveBulk: (data: [string, string][]) => Promise<void>;
}

interface Query {
    _key: string | { $in: string[] };
    score?: { $gte?: number; $lte?: number };
}

export = function (module: Module) {
    module.sortedSetRemove = async function (key: string | string[], value: string | string[]) {
        // Check if key is valid
        if (!key) {
            return;
        }
        const isValueArray = Array.isArray(value);
        // Check if val valid + val is arr of len > 0
        if (!value || (isValueArray && !value.length)) {
            return;
        }

        // Val is arr + len > 0
        if (isValueArray) {
            value = (value as string[]).map(helpers.valueToString);
        } else { // Val not arr
            value = helpers.valueToString(value);
        }

        await module.client.db().collection('objects').deleteMany({
            _key: Array.isArray(key) ? { $in: key } : key,
            value: isValueArray ? { $in: value } : value,
        });
    };

    module.sortedSetsRemove = async function (keys: string[], value: string) {
        if (!Array.isArray(keys) || !keys.length) {
            return;
        }
        value = helpers.valueToString(value);

        await module.client.db().collection('objects').deleteMany({ _key: { $in: keys }, value: value });
    };

    module.sortedSetsRemoveRangeByScore = async function (keys: string[], min: string, max: string) {
        if (!Array.isArray(keys) || !keys.length) {
            return;
        }
        const query: Query = { _key: { $in: keys } };
        if (keys.length === 1) {
            query._key = keys[0];
        }
        if (min !== '-inf') {
            query.score = { $gte: parseFloat(min) };
        }
        if (max !== '+inf') {
            query.score = query.score || {};
            query.score.$lte = parseFloat(max);
        }

        await module.client.db().collection('objects').deleteMany(query);
    };

    module.sortedSetRemoveBulk = async function (data: [string, string][]) {
        if (!Array.isArray(data) || !data.length) {
            return;
        }
        const bulk = module.client.db().collection('objects').initializeUnorderedBulkOp();
        data.forEach(item => bulk.find({ _key: item[0], value: String(item[1]) }).delete());
        await bulk.execute();
    };
}

