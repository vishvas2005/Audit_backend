/**
 * Application: NICSI Audit Log Backend Server
 * Description: Express.js server connected to MongoDB via Mongoose. 
 * Provides high-performance aggregation endpoints for the Search Audit Console dashboard.
 */

const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
app.use(cors());
const PORT = 3000;

app.use(express.json());

// MongoDB Connection Setup
mongoose.connect('mongodb://127.0.0.1:27017/nicsi_audit')
    .then(() => console.log('Connected to MongoDB'))
    .catch(err => console.error('connection error:', err));

// Flexible schema design mapped to the 'logs' collection without strict typing restrictions
const logSchema = new mongoose.Schema({}, { strict: false, collection: 'logs' });
const AuditLog = mongoose.model('AuditLog', logSchema);

/**
 * Endpoint: POST /api/reports/advanced-search
 * Description: Heavy enterprise aggregation pipeline using MongoDB $facet. 
 * Computes search metrics, service channels (API vs Portal), status outcomes, 
 * error distributions, and daily hit velocities simultaneously in a single query execution.
 */
app.post('/api/reports/advanced-search', async (req, res) => {
    try {
        const { user, status, type, dateTime, timeStr, startDate, endDate, channel } = req.body; 
        
        // Build dynamic MongoDB match stage based on filter parameters
        const matchStage = {};

        // Filter by specific user or unknown/system entities
        if (user) {
            if (user === 'SYSTEM_OR_UNKNOWN') {
                matchStage.userId = { $exists: false }; 
            } else {
                matchStage.userId = user;
            }
        }
        
        // Filter by specific search operation type
        if (type) {
            matchStage.searchType = type;
        }
        
        // Filter by success or failure outcome status
        if (status) {
            if (status.toUpperCase() === 'SUCCESS') {
                matchStage.outcome = 'SUCCESS';
            } else if (status.toUpperCase() === 'ERROR') {
                matchStage.outcome = { $ne: 'SUCCESS' };
            }
        }
        
        // Filter by traffic channel (API endpoints starting with /api vs Portal web traffic)
        if (channel) {
            if (channel === 'API') {
                matchStage.endpointCalled = { $regex: "^/api", $options: "i" };
            } else if (channel === 'PORTAL') {
                matchStage.endpointCalled = { $not: { $regex: "^/api", $options: "i" } };
            }
        }
        
        // Date range filtering (Enforces maximum query performance bounds for custom ranges)
        if (startDate && endDate) {
            matchStage.requestedOn = { 
                $gte: new Date(`${startDate}T00:00:00.000Z`), 
                $lte: new Date(`${endDate}T23:59:59.999Z`) 
            };
        } 
        else if (dateTime && timeStr) {
            let timeToUse = timeStr;
            if (timeToUse.length === 5) {
                timeToUse += ":00";
            }
            const specificTime = new Date(`${dateTime}T${timeToUse}.000Z`);
            const nextSecond = new Date(specificTime.getTime() + 1000);
            matchStage.requestedOn = {
                $gte: specificTime,
                $lt: nextSecond
            };
        } else if (dateTime && !timeStr) {
            const startOfDay = new Date(`${dateTime}T00:00:00.000Z`);
            const endOfDay = new Date(`${dateTime}T23:59:59.999Z`);
            matchStage.requestedOn = { 
                $gte: startOfDay, 
                $lte: endOfDay 
            };
        } else if (!dateTime && timeStr) {
            let timeToUse = timeStr;
            if (timeToUse.length === 5) {
                timeToUse += ":00";
            }
            const parts = timeToUse.split(':');
            const h = parseInt(parts[0], 10);
            const m = parseInt(parts[1], 10);
            const s = parseInt(parts[2], 10);
            
            matchStage.$expr = {
                $and: [
                    { $eq: [{ $type: "$requestedOn" }, "date"] },
                    { $eq: [{ $hour: "$requestedOn" }, h] },
                    { $eq: [{ $minute: "$requestedOn" }, m] },
                    { $eq: [{ $second: "$requestedOn" }, s] }
                ]
            };
        }

        // MongoDB $facet execution for parallel multi-faceted aggregations in one trip
        const rawReport = await AuditLog.aggregate([
            { $match: matchStage },
            {
                $facet: {
                    // Sub-pipeline 1: Search type distribution grouped per user
                    searchMetrics: [
                        {
                            $group: {
                                _id: {
                                    user: { $ifNull: ["$userId", "SYSTEM_OR_UNKNOWN"] },
                                    type: { $ifNull: ["$searchType", "UNKNOWN_SEARCH"] }
                                },
                                count: { $sum: 1 }
                            }
                        },
                        {
                            $group: {
                                _id: "$_id.user",
                                totalSearches: { $sum: "$count" },
                                searchBreakdown: {
                                    $push: {
                                        k: { $toString: "$_id.type" },
                                        v: "$count"
                                    }
                                }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                userId: "$_id",
                                totalSearches: 1,
                                searchBreakdown: { $arrayToObject: "$searchBreakdown" }
                            }
                        }
                    ],
                    // Sub-pipeline 2: Channel breakdown (API vs Portal traffic) per user
                    serviceMetrics: [
                        {
                            $group: {
                                _id: {
                                    user: { $ifNull: ["$userId", "SYSTEM_OR_UNKNOWN"] },
                                    service: {
                                        $cond: {
                                            if: { $regexMatch: { input: { $ifNull: ["$endpointCalled", ""] }, regex: "^/api" } },
                                            then: "API",
                                            else: "PORTAL"
                                        }
                                    }
                                },
                                count: { $sum: 1 }
                            }
                        },
                        {
                            $group: {
                                _id: "$_id.user",
                                serviceBreakdown: {
                                    $push: {
                                        k: "$_id.service",
                                        v: "$count"
                                    }
                                }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                userId: "$_id",
                                serviceBreakdown: { $arrayToObject: "$serviceBreakdown" }
                            }
                        }
                    ],
                    // Sub-pipeline 3: Status outcome summary (Success vs Error totals) per user
                    statusMetrics: [
                        {
                            $group: {
                                _id: {
                                    user: { $ifNull: ["$userId", "SYSTEM_OR_UNKNOWN"] },
                                    status: {
                                        $cond: {
                                            if: { $eq: ["$outcome", "SUCCESS"] },
                                            then: "SUCCESS",
                                            else: "ERROR"
                                        }
                                    }
                                },
                                count: { $sum: 1 }
                            }
                        },
                        {
                            $group: {
                                _id: "$_id.user",
                                statusBreakdown: {
                                    $push: {
                                        k: "$_id.status",
                                        v: "$count"
                                    }
                                }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                userId: "$_id",
                                statusBreakdown: { $arrayToObject: "$statusBreakdown" }
                            }
                        }
                    ],
                    // Sub-pipeline 4: Granular error category classification and count per user
                    errorMetrics: [
                        {
                            $match: { outcome: { $ne: "SUCCESS" } }
                        },
                        {
                            $group: {
                                _id: {
                                    user: { $ifNull: ["$userId", "SYSTEM_OR_UNKNOWN"] },
                                    errorIssue: { $ifNull: ["$errorMessage", { $ifNull: ["$outcome", "UNKNOWN_ERROR"] }] }
                                },
                                count: { $sum: 1 }
                            }
                        },
                        {
                            $group: {
                                _id: "$_id.user",
                                errorBreakdown: {
                                    $push: {
                                        k: { $toString: "$_id.errorIssue" },
                                        v: "$count"
                                    }
                                }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                userId: "$_id",
                                errorBreakdown: { $arrayToObject: "$errorBreakdown" }
                            }
                        }
                    ],
                    // Sub-pipeline 5: Time-series date grouping for hit velocity charts (%Y-%m-%d format)
                    dateMetrics: [
                        {
                            $group: {
                                _id: {
                                    user: { $ifNull: ["$userId", "SYSTEM_OR_UNKNOWN"] },
                                    dateStr: { 
                                        $dateToString: { 
                                            format: "%Y-%m-%d", 
                                            date: { $ifNull: ["$requestedOn", new Date("1970-01-01")] } 
                                        } 
                                    }
                                },
                                count: { $sum: 1 }
                            }
                        },
                        {
                            $group: {
                                _id: "$_id.user",
                                dateBreakdown: {
                                    $push: {
                                        k: "$_id.dateStr",
                                        v: "$count"
                                    }
                                }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                userId: "$_id",
                                dateBreakdown: { $arrayToObject: "$dateBreakdown" }
                            }
                        }
                    ]
                }
            }
        ]);

        // Post-processing: Stitching independent facet results into a unified structured JSON dictionary map
        const formattedReport = {};
        const searchArray = rawReport[0].searchMetrics;
        const serviceArray = rawReport[0].serviceMetrics;
        const statusArray = rawReport[0].statusMetrics;
        const errorArray = rawReport[0].errorMetrics;
        const dateArray = rawReport[0].dateMetrics;

        searchArray.forEach(item => {
            formattedReport[item.userId] = {
                totalSearches: item.totalSearches,
                searchBreakdown: item.searchBreakdown,
                serviceBreakdown: {},
                statusBreakdown: { SUCCESS: 0, ERROR: 0 },
                errorBreakdown: {},
                dateBreakdown: {}
            };
        });

        serviceArray.forEach(item => {
            if (formattedReport[item.userId]) {
                formattedReport[item.userId].serviceBreakdown = item.serviceBreakdown;
            } else {
                formattedReport[item.userId] = {
                    totalSearches: 0,
                    searchBreakdown: {},
                    serviceBreakdown: item.serviceBreakdown,
                    statusBreakdown: { SUCCESS: 0, ERROR: 0 },
                    errorBreakdown: {},
                    dateBreakdown: {}
                };
            }
        });

        statusArray.forEach(item => {
            if (formattedReport[item.userId]) {
                formattedReport[item.userId].statusBreakdown = {
                    SUCCESS: item.statusBreakdown.SUCCESS || 0,
                    ERROR: item.statusBreakdown.ERROR || 0
                };
            }
        });

        errorArray.forEach(item => {
            if (formattedReport[item.userId]) {
                formattedReport[item.userId].errorBreakdown = item.errorBreakdown;
            }
        });

        dateArray.forEach(item => {
            if (formattedReport[item.userId]) {
                formattedReport[item.userId].dateBreakdown = item.dateBreakdown;
            }
        });

        res.json(formattedReport);
    } catch (error) {
        console.error("Advanced Search Error:", error);
        res.status(500).json({ error: "Database query failed" });
    }
});

/**
 * Endpoint: POST /api/reports/user-query-details
 * Description: Fetches the latest 100 raw query log entries for a specific user and search type 
 * when an admin expands a category inside the User Details Drawer modal. Returns exact timestamps, 
 * searched payloads, and specific error message reasons.
 */
app.post('/api/reports/user-query-details', async (req, res) => {
    try {
        const { userId, searchType, startDate, endDate } = req.body;
        
        const matchStage = { userId, searchType };

        // Respect date filtering bounds if provided from the main dashboard context
        if (startDate && endDate) {
            matchStage.requestedOn = { 
                $gte: new Date(`${startDate}T00:00:00.000Z`), 
                $lte: new Date(`${endDate}T23:59:59.999Z`) 
            };
        }

        const exactSearches = await AuditLog.aggregate([
            { $match: matchStage },
            { $sort: { requestedOn: -1 } }, // Show newest records first
            { $limit: 100 },                // Performance cap limit set to 100 entries max
            {
                $project: {
                    _id: 0,
                    searchKey: "$searchKey", 
                    time: "$requestedOn",
                    status: "$outcome",
                    errorMessage: "$errorMessage"
                }
            }
        ]);

        res.json(exactSearches);
    } catch (error) {
        console.error("Detailed Query Error:", error);
        res.status(500).json({ error: "Failed to fetch exact queries" });
    }
});

app.listen(PORT, () => {
    console.log(`API running on http://localhost:${PORT}`);
});