using Eventuous.Diagnostics;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Registrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AxonIQ.Eventuous.Subscriptions;

public static class SubscriptionBuilderExtensions
{

    /// <summary>Use non-default checkpoint store</summary>
    /// <param name="builder"></param>
    /// <typeparam name="T">Checkpoint store type</typeparam>
    /// <returns></returns>
    public static SubscriptionBuilder<AxonServerStreamSubscription, AxonServerStreamSubscriptionOptions> UseCheckpointStore<T>(
      this SubscriptionBuilder<AxonServerStreamSubscription, AxonServerStreamSubscriptionOptions> builder)
      where T : class, ICheckpointStore
    {
      builder.Services.TryAddSingleton<T>();
      return EventuousDiagnostics.Enabled ? builder.AddParameterMap<ICheckpointStore, MeasuredCheckpointStore>(sp => new MeasuredCheckpointStore(sp.GetRequiredService<T>())) : builder.AddParameterMap<ICheckpointStore, T>();
    }

    /// <summary>Use non-default checkpoint store</summary>
    /// <param name="builder"></param>
    /// <typeparam name="T">Checkpoint store type</typeparam>
    /// <returns></returns>
    public static SubscriptionBuilder<AxonServerAllStreamSubscription, AxonServerAllStreamSubscriptionOptions> UseCheckpointStore<T>(
      this SubscriptionBuilder<AxonServerAllStreamSubscription, AxonServerAllStreamSubscriptionOptions> builder)
      where T : class, ICheckpointStore
    {
      builder.Services.TryAddSingleton<T>();
      return EventuousDiagnostics.Enabled ? builder.AddParameterMap<ICheckpointStore, MeasuredCheckpointStore>(sp => new MeasuredCheckpointStore(sp.GetRequiredService<T>())) : builder.AddParameterMap<ICheckpointStore, T>();
    }
}