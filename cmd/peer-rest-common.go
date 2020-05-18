/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

const (
	peerRESTVersion       = "v8"
	peerRESTVersionPrefix = SlashSeparator + peerRESTVersion
	peerRESTPrefix        = minioReservedBucketPath + "/peer"
	peerRESTPath          = peerRESTPrefix + peerRESTVersionPrefix
)

const (
	peerRESTMethodServerInfo                   = "/serverinfo"
	peerRESTMethodDriveOBDInfo                 = "/driveobdinfo"
	peerRESTMethodNetOBDInfo                   = "/netobdinfo"
	peerRESTMethodCPUOBDInfo                   = "/cpuobdinfo"
	peerRESTMethodDiskHwOBDInfo                = "/diskhwobdinfo"
	peerRESTMethodOsInfoOBDInfo                = "/osinfoobdinfo"
	peerRESTMethodMemOBDInfo                   = "/memobdinfo"
	peerRESTMethodProcOBDInfo                  = "/procobdinfo"
	peerRESTMethodDispatchNetOBDInfo           = "/dispatchnetobdinfo"
	peerRESTMethodDeleteBucket                 = "/deletebucket"
	peerRESTMethodServerUpdate                 = "/serverupdate"
	peerRESTMethodSignalService                = "/signalservice"
	peerRESTMethodBackgroundHealStatus         = "/backgroundhealstatus"
	peerRESTMethodGetLocks                     = "/getlocks"
	peerRESTMethodBucketPolicyRemove           = "/removebucketpolicy"
	peerRESTMethodLoadUser                     = "/loaduser"
	peerRESTMethodLoadServiceAccount           = "/loadserviceaccount"
	peerRESTMethodDeleteUser                   = "/deleteuser"
	peerRESTMethodDeleteServiceAccount         = "/deleteserviceaccount"
	peerRESTMethodLoadPolicy                   = "/loadpolicy"
	peerRESTMethodLoadPolicyMapping            = "/loadpolicymapping"
	peerRESTMethodDeletePolicy                 = "/deletepolicy"
	peerRESTMethodLoadGroup                    = "/loadgroup"
	peerRESTMethodStartProfiling               = "/startprofiling"
	peerRESTMethodDownloadProfilingData        = "/downloadprofilingdata"
	peerRESTMethodBucketPolicySet              = "/setbucketpolicy"
	peerRESTMethodBucketNotificationPut        = "/putbucketnotification"
	peerRESTMethodReloadFormat                 = "/reloadformat"
	peerRESTMethodCycleBloom                   = "/cyclebloom"
	peerRESTMethodTrace                        = "/trace"
	peerRESTMethodListen                       = "/listen"
	peerRESTMethodLog                          = "/log"
	peerRESTMethodBucketLifecycleSet           = "/setbucketlifecycle"
	peerRESTMethodBucketLifecycleRemove        = "/removebucketlifecycle"
	peerRESTMethodBucketEncryptionSet          = "/setbucketencryption"
	peerRESTMethodBucketEncryptionRemove       = "/removebucketencryption"
	peerRESTMethodPutBucketObjectLockConfig    = "/putbucketobjectlockconfig"
	peerRESTMethodBucketObjectLockConfigRemove = "/removebucketobjectlockconfig"
	peerRESTMethodPutBucketQuotaConfig         = "/putbucketquotaconfig"
	peerRESTMethodBucketQuotaConfigRemove      = "/removebucketquotaconfig"
)

const (
	peerRESTBucket        = "bucket"
	peerRESTUser          = "user"
	peerRESTGroup         = "group"
	peerRESTUserTemp      = "user-temp"
	peerRESTPolicy        = "policy"
	peerRESTUserOrGroup   = "user-or-group"
	peerRESTIsGroup       = "is-group"
	peerRESTUpdateURL     = "updateURL"
	peerRESTSha256Hex     = "sha256Hex"
	peerRESTLatestRelease = "latestReleaseTime"
	peerRESTSignal        = "signal"
	peerRESTProfiler      = "profiler"
	peerRESTDryRun        = "dry-run"
	peerRESTTraceAll      = "all"
	peerRESTTraceErr      = "err"

	peerRESTListenBucket = "bucket"
	peerRESTListenPrefix = "prefix"
	peerRESTListenSuffix = "suffix"
	peerRESTListenEvents = "events"
)
